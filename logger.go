package goscs

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
)

var logger zerolog.Logger
var isDev bool = strings.ToLower(os.Getenv("dev")) == "true" || os.Getenv("dev") == "1"

func init() {
	configureLogger("default", zerolog.InfoLevel)
}

func configureLogger(name string, level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
	if isDev {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Str("module", name).Timestamp().Caller().Logger()
	} else {
		logger = zerolog.New(os.Stdout).With().Str("module", name).Timestamp().Caller().Logger()
	}
}

func GetLoger(name string, level zerolog.Level) zerolog.Logger {
	if name == "" || level == 0 {
		// default logger
		return logger
	} else {
		configureLogger(name, level)
		return logger
	}
}
