.PHONY: release
release:
	goreleaser release --clean

commit = $(shell git rev-parse --short HEAD)
date = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
version = $(shell git describe --tags --abbrev=0 | sed 's/^v//')
.PHONY: docker-image
docker-image:
	docker build . -t ghcr.io/litesql/ha:$(version) --target production --build-arg COMMIT=$(commit) --build-arg DATE=$(date) --build-arg VERSION=$(version)

.PHONY: docker-image-dev
docker-image-dev:
	docker build . -t ghcr.io/litesql/ha:dev --target production --build-arg COMMIT=dev --build-arg DATE=$(date) --build-arg VERSION=dev

.PHONY: pages
pages:
	cd docs && bundle exec jekyll serve