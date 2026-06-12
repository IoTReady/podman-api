# Contributing

## Issues

Bug reports and feature requests are welcome on the [issue tracker](https://github.com/iotready/podman-api/issues). For bugs, include the podman-api version (`-version`), the podman version on the target host, and the relevant log lines.

## Pull requests

1. Fork the repo and create a branch from `main`.
2. `make test` must pass. `gofmt -l .` must be empty. `go vet ./...` must be clean.
3. Keep changes focused — one concern per PR.
4. Open the PR against `main`. Reference the issue it addresses.

## Build requirements

See the [Building](https://github.com/iotready/podman-api/wiki/Building) wiki page — the podman v5 bindings require specific build tags; a plain `go build` will fail without them.

## License

By contributing you agree that your changes will be licensed under the [MIT License](LICENSE).
