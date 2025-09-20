.PRONY: builder
builder:
	docker build -f Dockerfile-builder -t builder-ha .

.PHONY: release
release: builder
	docker run -e GITHUB_TOKEN=${GITHUB_TOKEN} builder-ha goreleaser release --clean
