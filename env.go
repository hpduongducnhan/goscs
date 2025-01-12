package goscs

import (
	"github.com/rs/zerolog/log"

	"github.com/spf13/viper"
)

type EnvStructConstraint interface {
	IsLoaded() bool
	SetIsLoaded(bool)
}

type BaseSettings struct {
	loaded bool
}

func (bs *BaseSettings) IsLoaded() bool {
	return bs.loaded
}

func (bs *BaseSettings) SetIsLoaded(isLoaded bool) {
	bs.loaded = isLoaded
}

func LoadEnvVars[T EnvStructConstraint](loadToStruct T, envFileName string, envPaths ...string) {
	if loadToStruct.IsLoaded() {
		return
	}
	for _, envPath := range envPaths {
		viper.AddConfigPath(envPath)
	}
	viper.SetConfigType("env")
	viper.SetConfigName(envFileName)

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal().Err(err).Str("loc", "0core0000").Msg("Error reading environment config file")
	}

	if err := viper.Unmarshal(loadToStruct); err != nil {
		log.Fatal().Err(err).Str("loc", "0core0001").Msg("Error Unmarshal environment config file")
	}
	loadToStruct.SetIsLoaded(true)
}
