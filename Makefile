# Thin wrapper over Maven + packaging/build-rpm.sh for convenient named
# targets -- it doesn't reimplement any build logic itself.
.PHONY: all jar test rpm clean help

all: jar

jar:
	mvn -q package

test:
	mvn test

rpm:
	./packaging/build-rpm.sh

clean:
	mvn -q clean

help:
	@echo "jar    build target/kscan.jar (default)"
	@echo "test   run the unit test suite"
	@echo "rpm    build the RPM via packaging/build-rpm.sh"
	@echo "clean  remove build output (mvn clean)"
