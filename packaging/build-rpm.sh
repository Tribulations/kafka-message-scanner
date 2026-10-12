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

# --release 8 at compile time only covers code compiled in THIS build; it
# can't catch a bundled dependency class that itself targets newer
# bytecode. This is the release gate for that: it fails the build, not
# just the first JDK-8 run that happens to exercise the offending class.
python3 packaging/check-bytecode.py target/kscan.jar

topdir="$repo_root/target/rpmbuild"
rm -rf "$topdir"
mkdir -p "$topdir"/{BUILD,RPMS,SRPMS,SOURCES,SPECS,BUILDROOT}

cp target/kscan.jar "$topdir/SOURCES/kscan.jar"
cp bin/kscan "$topdir/SOURCES/kscan"
# Deliberately NOT the dev-flavored scanner.properties at the repo root
# (topic=kc-hello, kafka.bootstrap.servers=localhost:9092) -- a production
# host has no safe guess for either, so the RPM ships a neutral template
# with both commented out, forcing an explicit --topic=/--config= choice.
cp packaging/scanner.properties "$topdir/SOURCES/scanner.properties"

rpmbuild \
    --define "_topdir $topdir" \
    --define "_version $version" \
    -bb packaging/kscan.spec

echo
echo "kscan: RPM(s) built:"
find "$topdir/RPMS" -name '*.rpm'
