We l# Pull the k6 binary from the official image and run it on a glibc-based
# runtime. The official image uses Alpine (musl) but the binary links against
# glibc, which causes "no such file or directory" on ARM64 Macs.
FROM mostafamoradian/xk6-kafka:latest AS source

FROM debian:bookworm-slim
COPY --from=source /usr/bin/k6 /usr/bin/k6
RUN chmod +x /usr/bin/k6
ENTRYPOINT ["k6"]