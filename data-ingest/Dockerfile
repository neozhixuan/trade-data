# ----------- Stage 1: Build -------------
FROM golang:1.22 AS builder

WORKDIR /app

COPY . .

# Build a static binary
# - CGO_ENABLED=0 removes the GCC compiler; Go programs typically use dynamic linking for C libraries
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main .

# ----------- Stage 2: Run -------------
# Alpine expects statically linked binaries (it uses musl-libc instead of glibc)
FROM alpine:latest

WORKDIR /root/

# Copy the binary over
COPY --from=builder /app/main .

# Double check for executability
RUN chmod +x ./main

CMD ["./main"]