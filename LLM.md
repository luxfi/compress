# LLM.md - Hanzo Compress

## Overview
Go module: github.com/luxfi/compress

## Tech Stack
- **Language**: Go

## Build & Run
```bash
go build ./...
go test ./...
```

## Structure
```
compress/
  LICENSE
  compressor.go
  compressor_test.go
  errors.go
  go.mod
  go.sum
  gzip_compressor.go
  gzip_zip_bomb.bin
  no_compressor.go
  no_compressor_test.go
  type.go
  type_test.go
  zstd_compressor.go
  zstd_zip_bomb.bin
```

## Key Files
- `go.mod` -- Go module definition
