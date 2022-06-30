include ../../Makefile.Common

.PHONY: build
build:
	cd ../../ && CGO_ENABLED=0 GOOS=linux go build -o ./local/otelcontribcol ./cmd/otelcontribcol

.PHONY: build-image
build-image: build
	docker build ../../local -f ../../cmd/otelcontribcol/Dockerfile -t ghcr.io/werf/telemetrywerfio-collector:dev

.PHONY: push-image
push-image:
	docker push ghcr.io/werf/telemetrywerfio-collector:dev
