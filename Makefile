.PHONY: build test test-integration fmt vet tidy

# The podman v5 bindings transitively pull in the storage graph drivers
# (btrfs, devicemapper) and gpgme, all of which need CGO and system -dev
# headers. We use only the remote libpod client, so we exclude those drivers
# and swap gpgme for the pure-Go OpenPGP implementation. Without these tags a
# clean `go build` fails on <btrfs/version.h> / missing gpgme.pc.
TAGS := containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper

build:
	go build -tags "$(TAGS)" -o bin/podman-api ./cmd/podman-api

test:
	go test -tags "$(TAGS)" ./...

test-integration:
	go test -tags "$(TAGS),integration" ./...

fmt:
	gofmt -w .

vet:
	go vet -tags "$(TAGS)" ./...

tidy:
	go mod tidy
