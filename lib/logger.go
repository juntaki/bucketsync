package bucketsync

import (
	"strings"

	"os"

	"go.uber.org/zap"
)

type Logger struct {
	*zap.Logger
}

func NewLogger(outputPath string, debug bool) (logger *Logger, err error) {
	file, err := os.OpenFile(outputPath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	file.Close()

	var config zap.Config
	if debug {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}
	config.OutputPaths = []string{outputPath}
	zapLogger, err := config.Build()
	if err != nil {
		return nil, err
	}

	_, err = zap.RedirectStdLogAt(zapLogger, zap.DebugLevel)
	if err != nil {
		return nil, err
	}

	logger = &Logger{
		Logger: zapLogger,
	}

	return logger, nil
}

// Wrap standard log library
func (l *Logger) Write(input []byte) (int, error) {
	l.Sugar().Debug(strings.Trim(string(input), "\n"))
	return len(input), nil
}

// For aws log library
func (l *Logger) Log(input ...interface{}) {
	l.Sugar().Debug(input)
}
