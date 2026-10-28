# Thin wrapper over Maven + packaging/build-rpm.sh for convenient named
# targets -- it doesn't reimplement any build logic itself.
.PHONY: all jar test rpm vendor clean help

all: jar

jar:
	mvn -q package

test:
	mvn test

rpm:
	./packaging/build-rpm.sh

vendor:
	./packaging/vendor-dependencies.sh

clean:
	mvn -q clean

help:
	@echo "jar    build target/kafka-inspect.jar (default)"
	@echo "test   run the unit test suite"
	@echo "rpm    build the RPM via packaging/build-rpm.sh"
	@echo "vendor vendor dependencies for an offline/air-gapped rpm build"
	@echo "clean  remove build output (mvn clean)"
