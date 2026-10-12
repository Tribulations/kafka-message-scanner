# Built via packaging/build-rpm.sh, which supplies _version (from VERSION)
# and _sourcedir (the staging directory it fills with kscan.jar, bin/kscan,
# and scanner.properties before invoking rpmbuild). Not meant to be built
# directly with a bare `rpmbuild -bb` against this file alone.
Name:           kscan
Version:        %{_version}
Release:        1%{?dist}
Summary:        Kafka topic/message scanner (debugging/analysis CLI)
License:        Proprietary
URL:            https://example.invalid/kscan
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
mkdir -p %{buildroot}/opt/kscan/bin
mkdir -p %{buildroot}/etc/kscan
mkdir -p %{buildroot}/usr/bin

install -m 0644 %{_sourcedir}/kscan.jar %{buildroot}/opt/kscan/kscan.jar
install -m 0755 %{_sourcedir}/kscan %{buildroot}/opt/kscan/bin/kscan
install -m 0644 %{_sourcedir}/scanner.properties %{buildroot}/etc/kscan/scanner.properties
ln -sf ../../opt/kscan/bin/kscan %{buildroot}/usr/bin/kscan

%files
%dir /opt/kscan
%dir /opt/kscan/bin
/opt/kscan/kscan.jar
/opt/kscan/bin/kscan
/usr/bin/kscan
%dir /etc/kscan
%config(noreplace) /etc/kscan/scanner.properties

%changelog
* Mon Oct 12 2026 kscan maintainers <kung_jocke@hotmail.com> - %{_version}-1
- See the project README/plan for phase-by-phase history; this spec does
  not track a line-by-line changelog.
