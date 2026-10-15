# Built via packaging/build-rpm.sh, which supplies _version (from VERSION)
# and _sourcedir (the staging directory it fills with kscan.jar, bin/kscan,
# and scanner.properties before invoking rpmbuild). Not meant to be built
# directly with a bare `rpmbuild -bb` against this file alone.
#
# Package name is descriptive (kafka-message-scanner) so the tool is easy to
# find via `rpm -qa`/`dnf search` on an already-provisioned host; the command
# it installs stays the short "kscan" (see bin/kscan) since that's what gets
# typed repeatedly during ad-hoc debugging.
Name:           kafka-message-scanner
Version:        %{_version}
Release:        1%{?dist}
Summary:        Kafka topic/message scanner (debugging/analysis CLI)
License:        Proprietary
URL:            https://example.invalid/kafka-message-scanner
BuildArch:      noarch

# Deliberately no "Requires: java-*" -- target hosts already provision
# Java 8 out-of-band, and bin/kscan's own JAVA_HOME resolution
# (KSCAN_JAVA_HOME env var, else trying a short list of known Java 8
# install paths) is the only thing that matters at runtime. See the
# README's "Build" section.

%description
kscan scans a Kafka topic (a bounded scan, or a live --follow tail) and
reports whether records exist matching a given key, header, and/or JSON
field value. It is a debugging/analysis tool only: it never joins a
consumer group and never commits offsets, so it is safe to run against
production topics without affecting any real consumer group.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/kafka-message-scanner/bin
mkdir -p %{buildroot}/etc/kafka-message-scanner
mkdir -p %{buildroot}/usr/bin

install -m 0644 %{_sourcedir}/kscan.jar %{buildroot}/opt/kafka-message-scanner/kscan.jar
install -m 0755 %{_sourcedir}/kscan %{buildroot}/opt/kafka-message-scanner/bin/kscan
install -m 0644 %{_sourcedir}/scanner.properties %{buildroot}/etc/kafka-message-scanner/scanner.properties
ln -sf ../../opt/kafka-message-scanner/bin/kscan %{buildroot}/usr/bin/kscan

%files
%dir /opt/kafka-message-scanner
%dir /opt/kafka-message-scanner/bin
/opt/kafka-message-scanner/kscan.jar
/opt/kafka-message-scanner/bin/kscan
/usr/bin/kscan
%dir /etc/kafka-message-scanner
%config(noreplace) /etc/kafka-message-scanner/scanner.properties

%changelog
* Mon Oct 12 2026 kafka-message-scanner maintainers <kung_jocke@hotmail.com> - %{_version}-1
- See the project README/plan for phase-by-phase history; this spec does
  not track a line-by-line changelog.
