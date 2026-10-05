# Multi-stage build for AltMount - Multi-platform optimized
# This is the self-contained development/local build Dockerfile
# For CI builds, use Dockerfile.ci which expects pre-built frontend assets
# Use buildkit syntax for advanced features
# syntax=docker/dockerfile:1.11

# Custom base stage with pre-installed packages
FROM lscr.io/linuxserver/baseimage-ubuntu:jammy AS custom-base

ARG DEBIAN_FRONTEND="noninteractive"

# Install mime-support and clean up in single layer
RUN apt-get update && \
    apt-get install --reinstall mime-support -y && \
    rm -rf /var/lib/apt/lists/*

# Frontend build stage
FROM oven/bun:1 AS frontend-builder

# Set working directory for frontend
WORKDIR /app/frontend

# Copy only dependency manifests first to leverage caching for bun install
COPY frontend/bun.lock frontend/package.json ./

# Install dependencies with enhanced cache mounting
RUN --mount=type=cache,target=/root/.bun/install/cache,sharing=locked \
    --mount=type=cache,target=/tmp/bun-cache,sharing=locked \
    bun install --frozen-lockfile

# Copy the rest of the frontend sources and build with cache
COPY frontend/ ./
RUN --mount=type=cache,target=/tmp/bun-build-cache,sharing=locked \
    bun run build

# Backend build stage with native arch support (no cross-compilation)
FROM golang:1.27-alpine AS backend-builder

# Install build dependencies including sqlite with cache mount
RUN --mount=type=cache,target=/var/cache/apk,sharing=locked \
    apk add --no-cache git gcc g++ musl-dev sqlite-dev

# Set working directory
WORKDIR /app

# Copy go module files first for better caching
COPY go.mod go.sum ./

# Download dependencies with enhanced cache mount
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/root/.cache/go-mod-download,sharing=locked \
    go mod download

# Copy source code in optimal order for caching
COPY ./pkg ./pkg
COPY ./internal ./internal
COPY ./cmd ./cmd
COPY ./frontend/embed_cli.go ./frontend/embed_cli.go
COPY ./frontend/embed_docker.go ./frontend/embed_docker.go

# Copy built frontend from previous stage
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

# Build web binary with enhanced cache mount for native architecture
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIMESTAMP=unknown
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    --mount=type=cache,target=/tmp/go-build-cache,sharing=locked \
    CGO_ENABLED=1 GOOS=linux \
    go build -a \
    -ldflags "-linkmode external -extldflags \"-static\" -X 'github.com/kipsilabs/altmount/internal/version.Version=${VERSION}' -X 'github.com/kipsilabs/altmount/internal/version.GitCommit=${COMMIT}' -X 'github.com/kipsilabs/altmount/internal/version.Timestamp=${BUILD_TIMESTAMP}'" \
    -o altmount cmd/altmount/main.go

# Final stage - use custom base image with pre-installed packages
ARG TARGETARCH
FROM custom-base

ARG DEBIAN_FRONTEND="noninteractive"
ARG BUILD_DATE
ARG VERSION
ARG PUID=1000
ARG PGID=1000

# Set up environment variables for PUID and PGID
ENV PUID=${PUID}
ENV PGID=${PGID}

# Set the working directory inside the container
WORKDIR /app

# Copy web binary from builder
COPY --from=backend-builder /app/altmount /app/altmount
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

# Install required packages for runtime (mime-support already in custom-base)
# docker.io provides docker CLI for the auto-update feature (requires /var/run/docker.sock mount)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        ca-certificates \
        docker.io && \
    rm -rf /var/lib/apt/lists/*

# Install rclone and FUSE support for internal mount functionality
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        fuse3 \
        curl \
        unzip && \
    echo "user_allow_other" >> /etc/fuse.conf && \
    case "$(uname -m)" in \
        x86_64) ARCH=amd64; SHA256=982b5aa772841168f8e380f139e9e787b2a105403e32b94da8676a0e1c0a13ab ;; \
        aarch64) ARCH=arm64; SHA256=03f2504174034b6d004152ed7369251c9a9ec1f7e0836eda420f5c7a5ec0dff9 ;; \
        armv7l) ARCH=arm; SHA256=6ea78cafa96a55f94da6a644d5057e8dc8ecfff9df0ab4011ce77e1fc654d114 ;; \
        *) echo "Unsupported architecture: $(uname -m)" && exit 1 ;; \
    esac && \
    curl -O "https://downloads.rclone.org/v1.75.1/rclone-v1.75.1-linux-${ARCH}.zip" && \
    echo "${SHA256}  rclone-v1.75.1-linux-${ARCH}.zip" | sha256sum -c - && \
    unzip "rclone-v1.75.1-linux-${ARCH}.zip" && \
    cp rclone-*/rclone /usr/local/bin/ && \
    chmod +x /usr/local/bin/rclone && \
    rm -rf rclone-* && \
    apt-get remove -y curl unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Make binary executable
RUN chmod +x /app/altmount

# Copy s6-overlay service configuration
COPY docker/root/ /

# Set environment variables
ENV PORT=8080
ENV HOST=0.0.0.0

# Expose port for web interface
EXPOSE 8080

# Volume
VOLUME ["/config", "/metadata"]

# Health check
HEALTHCHECK --interval=5s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:${PORT}/live || exit 1

# Labels
LABEL org.opencontainers.image.source="https://github.com/kipsilabs/altmount"
LABEL build_version="version: Build-date:- ${VERSION}  ${BUILD_DATE}"
LABEL maintainer="javi11"
