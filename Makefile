
#
# Default.
#

default: build

#
# Tasks.
#

# Build.
build: 
	@pip3 install .

# Dev.
dev:
	@python3 setup.py develop

# Deploy.
release:
	@python3 setup.py sdist upload

# Generate schema.
schema:
	@node generate-schema.js

# Test.
test:
	@python3 tests/test_tap_clubspeed.py

#
# Phonies.
#

.PHONY: build
.PHONY: dev
.PHONY: release
.PHONY: schema
.PHONY: test

