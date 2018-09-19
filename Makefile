
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

#
# Phonies.
#

.PHONY: test
.PHONY: build
.PHONY: dev

