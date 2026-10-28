# Built via packaging/build-rpm.sh, which supplies _version (from VERSION)
# and _sourcedir (the staging directory it fills with kafka-inspect.jar,
# bin/kafka-inspect, and scanner.properties before invoking rpmbuild). Not
# meant to be built directly with a bare `rpmbuild -bb` against this file
# alone.
#
# Package name, installed command (see bin/kafka-inspect), and /etc/ config
# directory are all "kafka-inspect" -- one consistent name everywhere, easy
# to find via `rpm -qa`/`dnf search`. Originally "kscan" (moved off that
# since it read as one more short Kerberos k-tool next to
# kinit/klist/kdestroy), then "kafka-scanner" with a separately descriptive
# package name ("kafka-message-scanner"); unified under "kafka-inspect" once
# the tool grew beyond pure message-scanning (see --describe).
Name:           kafka-inspect
Version:        %{_version}
Release:        4%{?dist }
Summary:        Kafka topic/message inspector (debugging/analysis CLI)
License:        Proprietary
URL:            https://example.invalid/kafka-inspect
BuildArch:      noarch

# Deliberately no "Requires: java-*" -- target hosts already provision
# Java 8 out-of-band, and bin/kafka-inspect's own JAVA_HOME resolution
# (KAFKA_INSPECT_JAVA_HOME env var, else trying a short list of known Java 8
# install paths) is the only thing that matters at runtime. See the
# README's "Build" section.

%description
kafka-inspect scans a Kafka topic (a bounded scan, or a live --follow tail)
and reports whether records exist matching a given key, header, and/or JSON
field value, or describes one (--describe: per-partition message count and
best-effort on-disk byte size). It is a debugging/analysis tool only: it never joins a
consumer group and never commits offsets, so it is safe to run against
production topics without affecting any real consumer group.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/kafka-inspect/bin
mkdir -p %{buildroot}/etc/kafka-inspect
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}%{_mandir}/man1

install -m 0644 %{_sourcedir}/kafka-inspect.jar %{buildroot}/opt/kafka-inspect/kafka-inspect.jar
install -m 0755 %{_sourcedir}/kafka-inspect %{buildroot}/opt/kafka-inspect/bin/kafka-inspect
install -m 0644 %{_sourcedir}/scanner.properties %{buildroot}/etc/kafka-inspect/scanner.properties
install -m 0644 %{_sourcedir}/scanner.properties.example %{buildroot}/etc/kafka-inspect/scanner.properties.example
install -m 0644 %{_sourcedir}/kafka-inspect.1 %{buildroot}%{_mandir}/man1/kafka-inspect.1
ln -sf ../../opt/kafka-inspect/bin/kafka-inspect %{buildroot}/usr/bin/kafka-inspect

%files
%dir /opt/kafka-inspect
%dir /opt/kafka-inspect/bin
/opt/kafka-inspect/kafka-inspect.jar
/opt/kafka-inspect/bin/kafka-inspect
/usr/bin/kafka-inspect
%dir /etc/kafka-inspect
%config(noreplace) /etc/kafka-inspect/scanner.properties
%config /etc/kafka-inspect/scanner.properties.example
%{_mandir}/man1/kafka-inspect.1*

%changelog
* Mon Oct 12 2026 kafka-inspect maintainers <kung_jocke@hotmail.com> - %{_version}-1
- See the project README/plan for phase-by-phase history; this spec does
  not track a line-by-line changelog.
