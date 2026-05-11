// Command podman-api is the HTTP service that translates CMS REST calls
// into libpod REST calls against one or more Podman hosts.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/templates"
)

func main() {
	var (
		addr        = flag.String("addr", "127.0.0.1:8080", "bind address for the API")
		metricsAddr = flag.String("metrics-addr", "", "if set, expose /metrics on this address (e.g. 127.0.0.1:9090); empty means no metrics endpoint")
		hostsDir    = flag.String("hosts-dir", "hosts", "directory of hosts/*.yaml files")
		keysFile    = flag.String("keys-file", "auth/keys.yaml", "path to bearer keys file")
		tmplDir     = flag.String("templates-dir", "", "if set, load templates from this dir instead of embedded")
	)
	flag.Parse()

	if len(flag.Args()) > 0 && flag.Arg(0) == "hash-token" {
		if len(flag.Args()) < 2 {
			fmt.Fprintln(os.Stderr, "usage: podman-api hash-token <plaintext>")
			os.Exit(2)
		}
		h, err := config.HashToken(flag.Arg(1))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(h)
		return
	}

	hosts, err := config.LoadHosts(*hostsDir)
	if err != nil {
		log.Fatalf("hosts: %v", err)
	}
	keys, fp, err := loadKeys(*keysFile)
	if err != nil {
		log.Fatalf("keys: %v", err)
	}
	keyStore := auth.NewKeyStore(keys)
	log.Printf("keys loaded: %d entries, fingerprint=%s", len(keys), fp)

	var tmpls []config.Template
	if *tmplDir != "" {
		tmpls, err = config.LoadTemplates(os.DirFS(*tmplDir), ".")
	} else {
		tmpls, err = config.LoadTemplates(templates.Files, ".")
	}
	if err != nil {
		log.Fatalf("templates: %v", err)
	}

	client, err := podman.NewReal(hosts)
	if err != nil {
		log.Fatalf("podman: %v", err)
	}

	svc := instance.NewService(client, hosts, tmpls)
	metrics := obs.New()
	audit := obs.NewAuditMiddleware(os.Stdout)

	// Compose metrics(audit(h)) so every guarded request is both measured
	// and audit-logged. The caller (main) composes rather than adding a
	// parameter to NewRouter.
	combined := func(h http.Handler) http.Handler {
		return metrics.Middleware()(audit(h))
	}

	// /metrics is never mounted on the main listener — operators must opt in
	// with -metrics-addr to bind it on a separate (typically internal) socket.
	router := api.NewRouter(svc, keyStore, combined, nil)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	var metricsSrv *http.Server
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", metrics.Handler())
		metricsSrv = &http.Server{
			Addr:              *metricsAddr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		}
	}

	// SIGHUP: re-read the keys file and atomically swap the live KeyStore.
	// A bad reload (parse error, no keys) is logged but does NOT kill the
	// running process, so a fat-fingered edit doesn't take the API down.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			newKeys, fp, err := loadKeys(*keysFile)
			if err != nil {
				log.Printf("keys reload FAILED, keeping previous set: %v", err)
				continue
			}
			if len(newKeys) == 0 {
				log.Printf("keys reload SKIPPED, file parsed but contained zero keys (path=%s, fp=%s)", *keysFile, fp)
				continue
			}
			keyStore.Store(newKeys)
			log.Printf("keys reloaded: %d entries, fingerprint=%s", len(newKeys), fp)
		}
	}()

	idleClosed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if metricsSrv != nil {
			_ = metricsSrv.Shutdown(ctx)
		}
		close(idleClosed)
	}()

	if metricsSrv != nil {
		go func() {
			log.Printf("metrics listening on %s", *metricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("metrics listener: %v", err)
			}
		}()
	}

	log.Printf("podman-api listening on %s with %d hosts, %d templates, %d keys",
		*addr, len(hosts), len(tmpls), len(keyStore.Load()))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-idleClosed
}

// loadKeys reads and parses the keys file, returning the parsed list and a
// short SHA-256 fingerprint of the file contents (for audit logs / operator
// confirmation that a SIGHUP picked up the intended edit).
func loadKeys(path string) ([]config.APIKey, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	keys, err := config.ParseKeysYAML(raw)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return keys, hex.EncodeToString(sum[:8]), nil
}
