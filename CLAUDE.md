# CLAUDE.md

Guidance for working in this repo.

## Forge: Forgejo (not GitHub)

This repo lives on a self-hosted **Forgejo** instance (`git.iotready.com`, SSH on
port 2222). `gh` does **not** work here. Use the `forgejo` CLI for issues and PRs:

```sh
forgejo issue create IoTReady/podman-api --title="..." --body="..."
forgejo issue view   IoTReady/podman-api 42
forgejo issue close  IoTReady/podman-api 42
forgejo pr create    IoTReady/podman-api --title="..." --head=<branch> --base=main --body="..."
forgejo pr merge     IoTReady/podman-api 17 --method=squash
forgejo <resource> --help    # detailed help
```

`OWNER/REPO` is always `IoTReady/podman-api`. Add `--json` for machine-readable output.

## Two remotes: Forgejo (private) + GitHub (public OSS)

This repo has two remotes:

```sh
git remote -v
# origin   ssh://git@git.iotready.com:2222/IoTReady/podman-api.git  (private, source of truth)
# github   git@github.com:IoTReady/podman-api.git              (public OSS mirror)
```

**All development happens on Forgejo. GitHub is a publication surface only.**

### `main` is PR-only, and GitHub never receives unreviewed code

1. **`main` is PR-only on Forgejo.** Direct commits/pushes to `main` are blocked —
   both by a Forgejo branch protection rule (`enable_push: false`,
   `apply_to_admins: true`, enforced server-side even for the repo owner) and by
   the local `pre-push` hook. Work on a feature branch and open a PR.

2. **Never push to GitHub until the change is reviewed and merged on Forgejo.**
   GitHub is a publication surface, not a review surface. Nothing — no commit, no
   release tag — goes to GitHub until it is already on Forgejo `main` via a merged
   PR. The `pre-push` hook refuses to push anything to GitHub that is not yet an
   ancestor of Forgejo `main`.

The correct flow, end to end:

```sh
git switch -c feat/my-change
git push -u origin feat/my-change
forgejo pr create IoTReady/podman-api --head=feat/my-change --base=main --body="..."
# ... review happens on Forgejo, then:
forgejo pr merge IoTReady/podman-api <n> --method=squash
# only now is it publishable:
git switch main && git pull --ff-only origin main
git push github main                 # publish reviewed, merged commits
git tag vX.Y.Z && git push origin vX.Y.Z && git push github vX.Y.Z   # tag AFTER merge
```

> **Never reuse a release tag.** Once pushed, a tag may be cached immutably in the
> public Go module proxy (`proxy.golang.org`). If a tag was pushed in error,
> delete it everywhere and bump to the **next** version — do not move it.

The pre-push hook (`.git-hooks/pre-push`) enforces both rules and runs `make test`
before any GitHub push. Install it once per clone:

```sh
cp .git-hooks/pre-push .git/hooks/pre-push && chmod +x .git/hooks/pre-push
```

Wiki changes go to both (`/tmp/podman-api-wiki` clone, push `main` to origin and `main:master` to github).

## Open-core contract

This repo is the **OSS core**. The commercial tier lives in a **separate private module**
(`git.iotready.com/IoTReady/podman-api-pro`) that imports this one as a Go dependency.

### Rules — read before touching either repo

1. **Commercial features never land here.** If a feature is commercial-only, it goes in
   `podman-api-pro`, not here.

2. **Commercial code only extends OSS — it never forks or patches it.** The commercial
   module imports `github.com/iotready/podman-api` at a tagged version and wires
   implementations into the published extension points (`extension.BlobStore`,
   `server.WithBlobStore`, etc.). It does not copy, re-implement, or shadow any OSS type.

3. **OSS fixes go here first.** If you need to fix something in the OSS core while working
   on the commercial tier, stop, fix it here, get it merged, tag a new release, then update
   the commercial module's `go.mod`. Never use a `replace` directive in committed
   `podman-api-pro` code — the pre-push hook in that repo will block it.

4. **Extension points are explicit contracts.** Adding a new seam (`extension/`, `server.With*`)
   is an OSS change — file an issue here, implement it here, release it, then consume it
   commercially. Don't add seams speculatively; add them when the commercial feature needs them.

### Adding a new extension point

```
1. File issue on IoTReady/podman-api describing the seam interface
2. Implement in extension/<name>.go (public interface)
3. Add server.With<Name>(impl extension.<Name>) Option
4. OSS default wired in server.RunWithFlags (same behaviour as before)
5. Merge, tag new OSS release, push to GitHub
6. Commercial module: go get github.com/iotready/podman-api@<new-tag>
7. Implement and wire the commercial backend in podman-api-pro
```

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

## Documentation

Operator docs live in both the Forgejo wiki (`git.iotready.com/IoTReady/podman-api/wiki`)
and the GitHub wiki (`github.com/IoTReady/podman-api/wiki`) — they are the same content,
kept in sync via `/tmp/podman-api-wiki` (a clone of the Forgejo wiki repo with `github`
as a second remote). The README is the quick reference and links into the GitHub wiki.

To update wiki pages:
```sh
cd /tmp/podman-api-wiki
# edit .md files
git add -A && git commit -m "docs: ..."
git push origin main          # Forgejo
git push github main:master   # GitHub
```

## Workflow conventions

- Feature work happens in git worktrees under `.worktrees/` (git-ignored).
- One PR per issue; PRs target `main` for review.
- Keep changes gofmt-clean (`gofmt -l .` must be empty) and `go vet` clean.
