# CLAUDE.md

Guidance for working in this repo.

## Forge: Forgejo (not GitHub)

This repo lives on a self-hosted **Forgejo** instance (`git.iotready.com`, SSH on
port 2222). `gh` does **not** work here. Use the `forgejo` CLI for issues and PRs:

```sh
forgejo issue create tej/podman-api --title="..." --body="..."
forgejo issue view   tej/podman-api 42
forgejo issue close  tej/podman-api 42
forgejo pr create    tej/podman-api --title="..." --head=<branch> --base=main --body="..."
forgejo pr merge     tej/podman-api 17 --method=squash
forgejo <resource> --help    # detailed help
```

`OWNER/REPO` is always `tej/podman-api`. Add `--json` for machine-readable output.

## Build / test

The binary transitively imports podman's CGO `btrfs`/`gpgme`/`devicemapper`
drivers. A plain `go build ./...` therefore **fails on a clean machine** without
those system headers. Always build and test with the remote-client build tags
(the `Makefile` carries them — prefer it):

```sh
make build                                 # -> bin/podman-api
make test                                  # unit tests
make test-integration                      # needs a real podman host

# equivalent raw invocation:
TAGS="containers_image_openpgp exclude_graphdriver_btrfs exclude_graphdriver_devicemapper"
go build -tags "$TAGS" -o bin/podman-api ./cmd/podman-api
```

Keep the tag list in `Makefile` and `.forgejo/workflows/ci.yaml` in sync.

## Workflow conventions

- Feature work happens in git worktrees under `.worktrees/` (git-ignored).
- One PR per issue; PRs target `main` for review.
- Keep changes gofmt-clean (`gofmt -l .` must be empty) and `go vet` clean.
