#!/usr/bin/env bash
#
# Builds target/rpmbuild/RPMS/noarch/kscan-<version>-1.noarch.rpm from the
# current source tree. VERSION (repo root) is the single source of truth for
# the version number -- this script is the only place that needs to read it;
# see pom.xml's "revision" property comment for why a plain "mvn package"
# doesn't need to know about this file at all.
set -euo pipefail

script_dir="$(dirname "$(readlink -f "$0")")"
repo_root="$(dirname "$script_dir")"
cd "$repo_root"

version="$(cat VERSION)"
echo "kscan: building version $version"

mvn -q package -Drevision="$version"

topdir="$repo_root/target/rpmbuild"
rm -rf "$topdir"
mkdir -p "$topdir"/{BUILD,RPMS,SRPMS,SOURCES,SPECS,BUILDROOT}

cp target/kscan.jar "$topdir/SOURCES/kscan.jar"
cp bin/kscan "$topdir/SOURCES/kscan"
cp scanner.properties "$topdir/SOURCES/scanner.properties"

rpmbuild \
    --define "_topdir $topdir" \
    --define "_version $version" \
    -bb packaging/kscan.spec

echo
echo "kscan: RPM(s) built:"
find "$topdir/RPMS" -name '*.rpm'
