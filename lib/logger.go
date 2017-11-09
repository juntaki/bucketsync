package bucketsync

import (
	"strings"

	"go.uber.org/zap"
)

type Logger struct {
	*zap.Logger
}

func NewLogger(outputPath string, debug bool) (logger *Logger, err error) {
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{outputPath}
	config.Development = debug

	zapLogger, err := config.Build()
	if err != nil {
		return nil, err
	}

	zap.RedirectStdLog(zapLogger)

	logger = &Logger{
		Logger: zapLogger,
	}

	//log.SetOutput(logger)
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
