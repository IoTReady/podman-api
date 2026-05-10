// Command podman-api is the HTTP service that translates CMS REST calls
// into libpod REST calls against one or more Podman hosts.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/templates"
)

func main() {
	var (
		addr     = flag.String("addr", "127.0.0.1:8080", "bind address")
		hostsDir = flag.String("hosts-dir", "hosts", "directory of hosts/*.yaml files")
		keysFile = flag.String("keys-file", "auth/keys.yaml", "path to bearer keys file")
		tmplDir  = flag.String("templates-dir", "", "if set, load templates from this dir instead of embedded")
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
	keysRaw, err := os.ReadFile(*keysFile)
	if err != nil {
		log.Fatalf("keys: %v", err)
	}
	keys, err := config.ParseKeysYAML(keysRaw)
	if err != nil {
		log.Fatalf("keys: %v", err)
	}

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

	router := api.NewRouter(svc, keys, combined, metrics.Handler())

	srv := &http.Server{
		Addr:              *addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	idleClosed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		close(idleClosed)
	}()

	log.Printf("podman-api listening on %s with %d hosts, %d templates, %d keys",
		*addr, len(hosts), len(tmpls), len(keys))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-idleClosed
}
