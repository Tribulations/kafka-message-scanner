#!/usr/bin/env bash
#
# Run ONCE, on a networked machine, to produce a self-contained bundle of
# every Maven dependency/plugin this project needs, for transfer to an
# air-gapped build host where packaging/build-rpm.sh will run. Deliberately
# does NOT touch the real ~/.m2/repository -- uses a disposable,
# project-local repo directory instead (-Dmaven.repo.local=), so the
# resulting tarball is exactly what this project needs and nothing else,
# never silently mixed with whatever a shared ~/.m2 already has on either
# machine. See the README's "Offline / air-gapped build" section for the
# full two-machine workflow.
set -euo pipefail

script_dir="$(dirname "$(readlink -f "$0")")"
repo_root="$(dirname "$script_dir")"
cd "$repo_root"

offline_repo="target/offline-repo"
version="$(cat VERSION)"

rm -rf "$offline_repo"
mkdir -p "$offline_repo"

echo "kafka-inspect: vendoring dependencies for an offline build into $offline_repo"
# dependency:go-offline alone isn't enough -- it resolves this project's own declared
# dependencies but not every internal component a build plugin instantiates at runtime (e.g.
# maven-resources-plugin's own MavenResourcesFiltering helper), so a later `mvn -o package`
# against just that would still fail with a "no implementation bound" plugin error. Running the
# real `package` lifecycle once, online, against this same throwaway repo guarantees everything
# the actual build touches ends up vendored, not just what go-offline assumes.
mvn -q package -Drevision="$version" -Dmaven.repo.local="$offline_repo"

tar_path="target/offline-repo.tar.gz"
tar -czf "$tar_path" -C target offline-repo

echo
echo "kafka-inspect: vendored dependency bundle ready: $tar_path"
echo "kafka-inspect: transfer this file, alongside the project repo itself, to the"
echo "       air-gapped build host. There, extract it so its contents land"
echo "       at target/offline-repo inside the repo, then run"
echo "       packaging/build-rpm.sh normally -- it auto-detects the vendored"
echo "       repo and builds against it instead of reaching out to the"
echo "       network."
