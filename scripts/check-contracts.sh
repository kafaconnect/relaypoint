#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

buf_bin="$repo_root/clients/typescript/node_modules/.bin/buf"
baseline="${1:-.git#branch=origin/main}"
tool_bin="$(mktemp -d)"

cleanup() {
  find "$tool_bin" -type f -delete
  rmdir "$tool_bin"
}
trap cleanup EXIT

"$buf_bin" lint
"$buf_bin" breaking --against "$baseline"

GOWORK=off GOBIN="$tool_bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
GOWORK=off GOBIN="$tool_bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$tool_bin:$PATH" GOWORK=off "$buf_bin" generate
bash scripts/generate_participation_addresses.sh --verify

git diff --exit-code -- gen/go clients/typescript/src/gen
untracked="$(git ls-files --others --exclude-standard -- gen/go clients/typescript/src/gen)"
if [[ -n "$untracked" ]]; then
  printf '%s\n' "$untracked"
  exit 1
fi

GOWORK=off go build ./gen/go/...
pnpm --dir clients/typescript typecheck
pnpm --dir clients/typescript build
pnpm --dir clients/typescript test
