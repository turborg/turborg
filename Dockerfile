# syntax=docker/dockerfile:1.7

# Build stage: pinned Go for reproducibility. Bump in lockstep with
# .github/workflows/ci.yml + go.mod's go directive.
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Cache module downloads independently of source changes so iterative
# builds only re-resolve when go.mod / go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 + -trimpath produces a static binary that runs on the
# distroless/static base. -s -w drops the symbol + debug tables for a
# ~30% smaller image.
ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w -X github.com/turborg/turborg/internal/version.Version=${VERSION}" \
    -o /turborg ./cmd/turborg

# turborg-server: the pooled multi-tenant runtime. Shipped in the same image
# so the hosted sidecar can run either binary — it overrides the entrypoint to
# /turborg-server on pooled hosts. Single-tenant / OSS use keeps /turborg.
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w -X github.com/turborg/turborg/internal/version.Version=${VERSION}" \
    -o /turborg-server ./cmd/turborg-server

# Runtime stage: distroless/static is ~2 MB and ships nothing but a
# CA bundle + tzdata. No shell, no package manager — minimal attack
# surface. /etc/passwd is included so the non-root user resolves.
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/turborg/turborg"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.description="turborg — modular AI agent framework"

COPY --from=builder /turborg /turborg
COPY --from=builder /turborg-server /turborg-server

# Listen on the WS gateway port by default; container orchestrators
# can publish or override as needed.
EXPOSE 8765

USER nonroot:nonroot
ENTRYPOINT ["/turborg"]
CMD ["run"]
