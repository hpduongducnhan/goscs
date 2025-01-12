package goscs

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
)

func ConfigureLogger(debug bool) {
	// zerolog allows for logging at the following levels (from highest to lowest):

	// panic (zerolog.PanicLevel, 5)
	// fatal (zerolog.FatalLevel, 4)
	// error (zerolog.ErrorLevel, 3)
	// warn (zerolog.WarnLevel, 2)
	// info (zerolog.InfoLevel, 1)
	// debug (zerolog.DebugLevel, 0)
	// trace (zerolog.TraceLevel, -1)

	// configure log time format
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// log.Debug().
	// 	Str("Scale", "833 cents").
	// 	Float64("Interval", 833.09).
	// 	Msg("Fibonacci is everywhere")

	// log.Debug().
	// 	Str("Name", "Tom").
	// 	Send()

	// pretty print
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

}
