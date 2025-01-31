package goscs

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
)

var logger *zerolog.Logger
var isDev bool = strings.ToLower(os.Getenv("dev")) == "true" || os.Getenv("dev") == "1"

func init() {
	configureLogger("default", zerolog.InfoLevel)
}

func configureLogger(name string, level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
	var _logger zerolog.Logger
	if isDev {
		_logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Str("module", name).Timestamp().Caller().Logger()
	} else {
		_logger = zerolog.New(os.Stdout).With().Str("module", name).Timestamp().Caller().Logger()
	}
	logger = &_logger
}

func setDefaultNameLevel(name string, level zerolog.Level) (string, zerolog.Level) {
	if name == "" {
		name = "default"
	}
	if level == 0 {
		level = zerolog.InfoLevel
	}
	return name, level
}

func GetLogger(name string, level zerolog.Level) zerolog.Logger {
	name, level = setDefaultNameLevel(name, level)
	if logger == nil {
		configureLogger(name, level)
	}
	return *logger
}

func NewLogger(name string, level zerolog.Level) zerolog.Logger {
	name, level = setDefaultNameLevel(name, level)
	configureLogger(name, level)
	return *logger
}
