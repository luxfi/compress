// Package compress provides compression utilities inspired by VictoriaMetrics
package compress

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/luxfi/metric"
)

// ZstdCompressor provides Zstandard compression similar to VictoriaMetrics
type ZstdCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
	compressCounter   *metric.OptimizedCounter
	decompressCounter *metric.OptimizedCounter
	bytesCompressed   *metric.OptimizedCounter
	bytesDecompressed *metric.OptimizedCounter
	compressTime      *metric.TimingMetric
	decompressTime    *metric.TimingMetric
}

// NewZstdCompressor creates a new Zstandard compressor
func NewZstdCompressor(level zstd.Level, reg *metric.MetricsRegistry) (*ZstdCompressor, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	compressor := &ZstdCompressor{
		encoder: encoder,
		decoder: decoder,
	}

	// Initialize metrics
	if reg != nil {
		compressor.compressCounter = metric.NewOptimizedCounter("compress_zstd_operations_total", "Total number of Zstd compression operations")
		compressor.decompressCounter = metric.NewOptimizedCounter("compress_zstd_operations_total", "Total number of Zstd decompression operations")
		compressor.bytesCompressed = metric.NewOptimizedCounter("compress_bytes_total", "Total number of bytes compressed")
		compressor.bytesDecompressed = metric.NewOptimizedCounter("compress_bytes_total", "Total number of bytes decompressed")

		compressHistogram := metric.NewOptimizedHistogram("compress_duration_seconds", "Duration of compression operations", metric.DefBuckets)
		decompressHistogram := metric.NewOptimizedHistogram("compress_duration_seconds", "Duration of decompression operations", metric.DefBuckets)
		compressor.compressTime = metric.NewTimingMetric(compressHistogram)
		compressor.decompressTime = metric.NewTimingMetric(decompressHistogram)

		reg.RegisterCounter("compress_zstd_compress_ops", compressor.compressCounter)
		reg.RegisterCounter("compress_zstd_decompress_ops", compressor.decompressCounter)
		reg.RegisterCounter("compress_bytes_compressed", compressor.bytesCompressed)
		reg.RegisterCounter("compress_bytes_decompressed", compressor.bytesDecompressed)
		reg.RegisterHistogram("compress_compress_duration", compressHistogram)
		reg.RegisterHistogram("compress_decompress_duration", decompressHistogram)
	}

	return compressor, nil
}

// Compress compresses the input data using Zstd
func (zc *ZstdCompressor) Compress(input []byte) ([]byte, error) {
	if zc.compressTime != nil {
		zc.compressTime.Reset()
		defer zc.compressTime.Stop()
	}

	result := zc.encoder.EncodeAll(input, nil)

	if zc.compressCounter != nil {
		zc.compressCounter.Inc()
	}
	if zc.bytesCompressed != nil {
		zc.bytesCompressed.Add(float64(len(input)))
	}

	return result, nil
}

// Decompress decompresses the input data using Zstd
func (zc *ZstdCompressor) Decompress(input []byte) ([]byte, error) {
	if zc.decompressTime != nil {
		zc.decompressTime.Reset()
		defer zc.decompressTime.Stop()
	}

	result, err := zc.decoder.DecodeAll(input, nil)
	if err != nil {
		return nil, err
	}

	if zc.decompressCounter != nil {
		zc.decompressCounter.Inc()
	}
	if zc.bytesDecompressed != nil {
		zc.bytesDecompressed.Add(float64(len(result)))
	}

	return result, nil
}

// CompressLevel compresses with a specific level
func CompressLevel(input []byte, level zstd.Level) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()

	return encoder.EncodeAll(input, nil), nil
}

// DecompressSafe decompresses with size limits to prevent decompression bombs
func DecompressSafe(input []byte, maxSize int) ([]byte, error) {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(maxSize)))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	result, err := decoder.DecodeAll(input, nil)
	if err != nil {
		return nil, err
	}

	if len(result) > maxSize {
		return nil, io.ErrShortBuffer
	}

	return result, nil
}

// StreamCompressor provides streaming compression
type StreamCompressor struct {
	writer  io.WriteCloser
	reader  io.Reader
	buf     *bytes.Buffer
	metrics *ZstdCompressor
}

// NewStreamCompressor creates a new streaming compressor
func NewStreamCompressor(level zstd.Level, reg *metric.MetricsRegistry) (*StreamCompressor, error) {
	metrics, err := NewZstdCompressor(level, reg)
	if err != nil {
		return nil, err
	}

	return &StreamCompressor{
		metrics: metrics,
		buf:     &bytes.Buffer{},
	}, nil
}

// CompressStream compresses data from a reader to a writer
func (sc *StreamCompressor) CompressStream(in io.Reader, out io.Writer) error {
	if sc.metrics.compressTime != nil {
		sc.metrics.compressTime.Reset()
		defer sc.metrics.compressTime.Stop()
	}

	encoder, err := zstd.NewWriter(out)
	if err != nil {
		return err
	}
	defer encoder.Close()

	_, err = io.Copy(encoder, in)
	if err != nil {
		return err
	}

	if sc.metrics.compressCounter != nil {
		sc.metrics.compressCounter.Inc()
	}

	return encoder.Close()
}

// DecompressStream decompresses data from a reader to a writer
func (sc *StreamCompressor) DecompressStream(in io.Reader, out io.Writer) error {
	if sc.metrics.decompressTime != nil {
		sc.metrics.decompressTime.Reset()
		defer sc.metrics.decompressTime.Stop()
	}

	decoder, err := zstd.NewReader(in)
	if err != nil {
		return err
	}
	defer decoder.Close()

	_, err = io.Copy(out, decoder)
	if err != nil {
		return err
	}

	if sc.metrics.decompressCounter != nil {
		sc.metrics.decompressCounter.Inc()
	}

	return nil
}

// Close closes the stream compressor
func (sc *StreamCompressor) Close() error {
	return nil
}

// CompressionStats provides statistics about compression performance
type CompressionStats struct {
	OriginalSize    int64
	CompressedSize  int64
	CompressionRatio float64
	CompressionTime int64 // in nanoseconds
}

// CalculateCompressionRatio calculates the compression ratio
func CalculateCompressionRatio(original, compressed int) float64 {
	if original == 0 {
		return 1.0
	}
	return float64(compressed) / float64(original)
}