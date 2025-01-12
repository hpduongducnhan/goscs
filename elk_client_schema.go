package goscs

import (
	"io"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/rs/zerolog/log"
)

type InnerHit struct {
	ID     string                 `json:"_id"`
	Index  string                 `json:"_index"`
	Source map[string]interface{} `json:"_source"`
	Sort   []float64              `json:"sort"`
}

type ElkHits struct {
	Hits []InnerHit `json:"hits"`
}

type ElkResponse struct {
	ScrollID string  `json:"_scroll_id"`
	Hits     ElkHits `json:"hits"`
}

func parseElkResp(elkResp *esapi.Response) (*ElkResponse, error) {
	var response ElkResponse
	defer elkResp.Body.Close()

	// log.Printf("Parsing response %s", elkResp)
	bodyBytes, err := io.ReadAll(elkResp.Body)
	if err != nil {
		log.Error().Err(err).Str("elkClient", "parseResp").Msg("error reading response body")
		return nil, err
	}

	// Parse JSON into the struct
	err = json.Unmarshal(bodyBytes, &response)
	if err != nil {
		log.Error().Err(err).Str("elkClient", "parseResp").Msg("error unmarshaling JSON")
		return nil, err
	}

	return &response, nil

}
