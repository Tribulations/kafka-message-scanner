#!/usr/bin/env python3
"""
Fails (exit 1) if any .class file in the shaded jar targets bytecode newer
than Java 8 (major version 52) -- the one thing `--release 8` at compile
time can't fully guarantee by itself for bundled dependency classes.

Excluded on purpose:
  - META-INF/versions/**  (multi-release jar overlays: only loaded by a JVM
    new enough to use them in the first place, never by Java 8)
  - module-info.class     (a JPMS module descriptor some dependencies ship
    for JDK 9+ build tooling; the Java 8 classloader never reads it)
"""
import struct
import sys
import zipfile

JAVA_8_MAJOR_VERSION = 52


def is_checked(name):
    if not name.endswith(".class"):
        return False
    if name.startswith("META-INF/versions/"):
        return False
    if name.endswith("module-info.class"):
        return False
    return True


def major_version(zf, name):
    with zf.open(name) as f:
        header = f.read(8)
    # u4 magic (0xCAFEBABE), u2 minor, u2 major
    _, _, major = struct.unpack(">IHH", header)
    return major


def main():
    if len(sys.argv) != 2:
        print("usage: check-bytecode.py <jar-path>", file=sys.stderr)
        return 2

    jar_path = sys.argv[1]
    offenders = []

    with zipfile.ZipFile(jar_path) as zf:
        for info in zf.infolist():
            if not is_checked(info.filename):
                continue
            major = major_version(zf, info.filename)
            if major > JAVA_8_MAJOR_VERSION:
                offenders.append((info.filename, major))

    if offenders:
        print(f"kscan: {len(offenders)} class(es) in {jar_path} target newer than Java 8:",
              file=sys.stderr)
        for name, major in offenders[:20]:
            print(f"  {name}: class file major version {major}", file=sys.stderr)
        if len(offenders) > 20:
            print(f"  ... and {len(offenders) - 20} more", file=sys.stderr)
        return 1

    print(f"kscan: all checked classes in {jar_path} are Java 8 bytecode (major <= {JAVA_8_MAJOR_VERSION})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
