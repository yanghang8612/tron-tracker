package log

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logTmFmt = "2006-01-02 15:04:05"
)

func init() {
	Encoder := getEncoder()
	WriteSyncer := getWriteSyncer()
	LevelEnabler := getLevelEnabler()
	// ConsoleEncoder := getConsoleEncoder()
	newCore := zapcore.NewTee(
		zapcore.NewCore(Encoder, WriteSyncer, LevelEnabler), // 写入文件
		// zapcore.NewCore(ConsoleEncoder, zapcore.Lock(os.Stdout), zapcore.DebugLevel), // 写入控制台
	)
	logger := zap.New(newCore, zap.AddCaller())
	zap.ReplaceGlobals(logger)
	logger.Info("log init completed")
	fmt.Println("log init completed")
}

func getEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(
		zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller_line",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     "",
			EncodeLevel:    cEncodeLevel,
			EncodeTime:     cEncodeTime,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   cEncodeCaller,
		})
}

func getConsoleEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())
}

// GetWriteSyncer 自定义的WriteSyncer
func getWriteSyncer() zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   "./app.log",
		MaxSize:    200,
		MaxBackups: 10,
		MaxAge:     30,
	}
	return zapcore.AddSync(lumberJackLogger)
}

// GetLevelEnabler 自定义的LevelEnabler
func getLevelEnabler() zapcore.Level {
	return zapcore.DebugLevel
}

// cEncodeLevel 自定义日志级别显示
func cEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

// cEncodeTime 自定义时间格式显示
func cEncodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + t.Format(logTmFmt) + "]")
}

// cEncodeCaller 自定义行号显示
func cEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + caller.TrimmedPath() + "]")
}

func GinLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()

		cost := time.Since(start)
		zap.L().Info(path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.String("errors", c.Errors.ByType(gin.ErrorTypePrivate).String()),
			zap.Duration("cost", cost),
		)
	}
}

// func Debug(msg string, ctx ...interface{}) {
// 	sugar.Debugf(msg+"\n", ctx...)
// }
//
// func Info(msg string, ctx ...interface{}) {
// 	sugar.Infof(msg, ctx...)
// }
//
// func Warn(msg string, ctx ...interface{}) {
// 	sugar.Warnf(msg, ctx...)
// }
//
// func Error(msg string, ctx ...interface{}) {
// 	sugar.Errorf(msg, ctx...)
// }
//
// func DPanic(msg string, ctx ...interface{}) {
// 	sugar.DPanicf(msg, ctx...)
// }
//
// func Panic(msg string, ctx ...interface{}) {
// 	sugar.Panicf(msg, ctx...)
// 	os.Exit(1)
// }
