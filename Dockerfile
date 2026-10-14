# Built by .github/workflows/release.yml from the release's static musl
# binaries. distroless/static carries CA certificates, which the TLS stack
# reads at startup.
FROM gcr.io/distroless/static-debian12:nonroot
ARG TARGETARCH
COPY bin/${TARGETARCH}/freighter /usr/local/bin/freighter
ENTRYPOINT ["/usr/local/bin/freighter"]
