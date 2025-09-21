.PRONY: builder
builder:
	docker build -f Dockerfile-builder -t builder-ha .

.PHONY: release
release: builder
	docker run -e GITHUB_TOKEN=${GITHUB_TOKEN} builder-ha goreleaser release --clean

commit = $(shell git rev-parse --short HEAD)
date = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
version = $(shell git describe --tags --abbrev=0 | sed 's/^v//')
.PHONY: docker-image
docker-image:
	docker build . -t ghcr.io/litesql/ha:$(version) --target production --build-arg COMMIT=$(commit) --build-arg DATE=$(date) --build-arg VERSION=$(version)