#!/usr/bin/env bash
#
# Builds target/rpmbuild/RPMS/noarch/kafka-inspect-<version>-1.noarch.rpm from the
# current source tree. VERSION (repo root) is the single source of truth for
# the version number -- this script is the only place that needs to read it;
# see pom.xml's "revision" property comment for why a plain "mvn package"
# doesn't need to know about this file at all.
set -euo pipefail

script_dir="$(dirname "$(readlink -f "$0")")"
repo_root="$(dirname "$script_dir")"
cd "$repo_root"

version="$(cat VERSION)"
echo "kafka-inspect: building version $version"

# If packaging/vendor-dependencies.sh was already run (on a networked machine) and its
# target/offline-repo.tar.gz was extracted here, build against that vendored repo instead of
# reaching out to the network -- this is the only thing an air-gapped build host needs, since
# mvn/rpmbuild themselves are assumed already installed there (see README's "Offline / air-gapped
# build" section). Auto-detected rather than a separate flag: the vendored repo's mere presence is
# itself the clearest signal of intent, and this keeps the common networked-machine path unchanged.
mvn_args=(-q package -Drevision="$version")
offline_repo="target/offline-repo"
if [ -d "$offline_repo" ]; then
    echo "kafka-inspect: vendored dependency repo found at $offline_repo -- building offline"
    mvn_args+=(-o "-Dmaven.repo.local=$offline_repo")
fi

mvn "${mvn_args[@]}"

# --release 8 at compile time only covers code compiled in THIS build; it
# can't catch a bundled dependency class that itself targets newer
# bytecode. This is the release gate for that: it fails the build, not
# just the first JDK-8 run that happens to exercise the offending class.
python3 packaging/check-bytecode.py target/kafka-inspect.jar

topdir="$repo_root/target/rpmbuild"
rm -rf "$topdir"
mkdir -p "$topdir"/{BUILD,RPMS,SRPMS,SOURCES,SPECS,BUILDROOT}

cp target/kafka-inspect.jar "$topdir/SOURCES/kafka-inspect.jar"
cp bin/kafka-inspect "$topdir/SOURCES/kafka-inspect"
# Deliberately NOT the dev-flavored scanner.properties at the repo root
# (topic=kc-hello, kafka.bootstrap.servers=localhost:9092) -- a production
# host has no safe guess for either, so the RPM ships a neutral template
# with both commented out, forcing an explicit --topic=/--config= choice.
cp packaging/scanner.properties "$topdir/SOURCES/scanner.properties"
cp packaging/scanner.properties.example "$topdir/SOURCES/scanner.properties.example"
cp packaging/kafka-inspect.1 "$topdir/SOURCES/kafka-inspect.1"

rpmbuild \
    --define "_topdir $topdir" \
    --define "_version $version" \
    -bb packaging/kafka-inspect.spec

echo
echo "kafka-inspect: RPM(s) built:"
find "$topdir/RPMS" -name '*.rpm'
