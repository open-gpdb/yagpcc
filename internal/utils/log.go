package utils

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DualLog returns a logger that writes JSON log lines only to f (not stdout),
// so test runs (e.g. Ginkgo) stay quiet on the console while trace.log keeps full detail.
func DualLog(debug bool, f *os.File) *zap.SugaredLogger {
	pe := zap.NewProductionEncoderConfig()
	pe.EncodeTime = zapcore.ISO8601TimeEncoder
	fileEncoder := zapcore.NewJSONEncoder(pe)

	level := zap.InfoLevel
	if debug {
		level = zap.DebugLevel
	}

	core := zapcore.NewCore(fileEncoder, zapcore.AddSync(f), level)
	return zap.New(core).Sugar()
}
