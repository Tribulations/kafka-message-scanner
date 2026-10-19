# Built via packaging/build-rpm.sh, which supplies _version (from VERSION)
# and _sourcedir (the staging directory it fills with kscan.jar,
# bin/kafka-scanner, and scanner.properties before invoking rpmbuild). Not
# meant to be built directly with a bare `rpmbuild -bb` against this file
# alone.
#
# Package name is descriptive (kafka-message-scanner) so the tool is easy to
# find via `rpm -qa`/`dnf search` on an already-provisioned host; the
# installed command is "kafka-scanner" (see bin/kafka-scanner) -- not the
# shorter "kscan" originally used, since that read as one more Kerberos
# k-tool next to kinit/klist/kdestroy.
Name:           kafka-message-scanner
Version:        %{_version}
Release:        1%{?dist}
Summary:        Kafka topic/message scanner (debugging/analysis CLI)
License:        Proprietary
URL:            https://example.invalid/kafka-message-scanner
BuildArch:      noarch

# Deliberately no "Requires: java-*" -- target hosts already provision
# Java 8 out-of-band, and bin/kafka-scanner's own JAVA_HOME resolution
# (KAFKA_SCANNER_JAVA_HOME env var, else trying a short list of known Java 8
# install paths) is the only thing that matters at runtime. See the
# README's "Build" section.

%description
kafka-scanner scans a Kafka topic (a bounded scan, or a live --follow tail)
and reports whether records exist matching a given key, header, and/or JSON
field value. It is a debugging/analysis tool only: it never joins a
consumer group and never commits offsets, so it is safe to run against
production topics without affecting any real consumer group.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/kafka-message-scanner/bin
mkdir -p %{buildroot}/etc/kafka-message-scanner
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}%{_mandir}/man1

install -m 0644 %{_sourcedir}/kscan.jar %{buildroot}/opt/kafka-message-scanner/kscan.jar
install -m 0755 %{_sourcedir}/kafka-scanner %{buildroot}/opt/kafka-message-scanner/bin/kafka-scanner
install -m 0644 %{_sourcedir}/scanner.properties %{buildroot}/etc/kafka-message-scanner/scanner.properties
install -m 0644 %{_sourcedir}/kafka-scanner.1 %{buildroot}%{_mandir}/man1/kafka-scanner.1
ln -sf ../../opt/kafka-message-scanner/bin/kafka-scanner %{buildroot}/usr/bin/kafka-scanner

%files
%dir /opt/kafka-message-scanner
%dir /opt/kafka-message-scanner/bin
/opt/kafka-message-scanner/kscan.jar
/opt/kafka-message-scanner/bin/kafka-scanner
/usr/bin/kafka-scanner
%dir /etc/kafka-message-scanner
%config(noreplace) /etc/kafka-message-scanner/scanner.properties
%{_mandir}/man1/kafka-scanner.1*

%changelog
* Mon Oct 12 2026 kafka-message-scanner maintainers <kung_jocke@hotmail.com> - %{_version}-1
- See the project README/plan for phase-by-phase history; this spec does
  not track a line-by-line changelog.
