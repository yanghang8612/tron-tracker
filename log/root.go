package log

import (
	"strings"
	"time"

	"tron-tracker/config"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Init(cfg *config.LogConfig) {
	Encoder := getEncoder()
	WriteSyncer := getWriteSyncer(cfg.Path + "/" + cfg.File)
	LevelEnabler := getLevelEnabler(cfg.Level)
	newCore := zapcore.NewTee(
		zapcore.NewCore(Encoder, WriteSyncer, LevelEnabler),
	)

	logger := zap.New(newCore, zap.AddCaller())
	zap.ReplaceGlobals(logger)
}

func getEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(
		zapcore.EncoderConfig{
			TimeKey:  "ts",
			LevelKey: "level",
			NameKey:  "logger",
			// CallerKey:      "caller_line",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     "",
			EncodeLevel:    cEncodeLevel,
			EncodeTime:     cEncodeTime,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			// EncodeCaller:   cEncodeCaller,
			ConsoleSeparator: " ",
		})
}

func getWriteSyncer(filename string) zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    200,
		MaxBackups: 10,
		MaxAge:     30,
	}
	return zapcore.AddSync(lumberJackLogger)
}

func getLevelEnabler(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "panic":
		return zapcore.PanicLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

func cEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(level.CapitalString())
}

func cEncodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + t.Format("2006-01-02 15:04:05.000") + "]")
}

func cEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + caller.TrimmedPath() + "]")
}
