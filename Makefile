.PHONY: release
release:
	goreleaser release --clean --rm-dist

commit = $(shell git rev-parse --short HEAD)
date = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
version = $(shell git describe --tags --abbrev=0 | sed 's/^v//')
.PHONY: docker-image
docker-image:
	docker build . -t ghcr.io/litesql/ha:$(version) --target production --build-arg COMMIT=$(commit) --build-arg DATE=$(date) --build-arg VERSION=$(version)

.PHONY: pages
pages:
	cd docs && bundle exec jekyll serve