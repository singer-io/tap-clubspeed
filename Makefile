
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

#
# Phonies.
#

.PHONY: test
.PHONY: build
.PHONY: dev
.PHONY: schema

