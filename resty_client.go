package goscs

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	resty "github.com/go-resty/resty/v2"
)

func NewClient(proxyAdd string) *resty.Client {
	client := resty.New()
	client.SetRetryCount(3)
	client.SetRetryWaitTime(5 * time.Second)
	client.SetRetryMaxWaitTime(20 * time.Second)

	client.SetJSONMarshaler(json.Marshal)
	client.SetJSONUnmarshaler(json.Unmarshal)

	if proxyAdd != "" {
		client.SetProxy(proxyAdd)
	}
	return client
}

func SaveApiResponse(filePath string, resp *resty.Response) error {
	// save response to file
	if !strings.HasSuffix(filePath, ".json") {
		filePath += ".json"
	}

	// process response
	var jsonResponse string
	statusCode := resp.StatusCode()
	if 200 <= statusCode && statusCode <= 299 {
		var jsonResp map[string]interface{}
		if err := json.Unmarshal(resp.Body(), &jsonResp); err != nil {
			log.Error().Err(err).Msg("Error unmarshalling JSON response")
			return err
		}
		encodedJSON, err := json.MarshalIndent(jsonResponse, "", "    ")
		if err != nil {
			log.Error().Err(err).Msg("Error encoding JSON")
			return err
		}
		// log.Printf("JSON response: %s", string(encodedJSON))
		jsonResponse = string(encodedJSON)
	} else {
		jsonResponse = resp.String()
	}

	// Open the file for writing, create it if it doesn't exist
	file, err := os.Create(filePath)
	if err != nil {
		log.Error().Err(err).Msg("Error creating file:")
		return err
	}
	defer file.Close()

	// Write the string to the file
	_, err = io.WriteString(file, jsonResponse)
	if err != nil {
		log.Error().Err(err).Msg("Error writing to file:")
		return err
	}
	return nil
}
