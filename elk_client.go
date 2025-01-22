package goscs

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/rs/zerolog/log"
)

var elkScrollTime time.Duration = 2 * time.Minute

type EslasticClient[MsgType any] struct {
	Addresses    []string
	AuthUser     string
	AuthPassword string
	client       *elasticsearch.Client
}

func (esc *EslasticClient[MsgType]) getDefaultConfig() *elasticsearch.Config {
	return &elasticsearch.Config{
		Addresses:     esc.Addresses,
		Username:      esc.AuthUser,
		Password:      esc.AuthPassword,
		RetryOnStatus: []int{502, 503, 504},
		MaxRetries:    3,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
}
func (esc *EslasticClient[MsgType]) clearScroll(scrollID string) {
	esClient := esc.client
	_, err := esClient.ClearScroll(
		esClient.ClearScroll.WithScrollID(scrollID),
	)
	if err != nil {
		log.Error().Err(err).Msg("error clearing scroll")
	}
}

func (esc *EslasticClient[MsgType]) Connect(config *elasticsearch.Config) error {
	if len(esc.Addresses) == 0 {
		return fmt.Errorf("no Elasticsearch addresses provided")
	}
	if config == nil {
		config = esc.getDefaultConfig()
	}
	es, err := elasticsearch.NewClient(*config)
	if err != nil {
		return nil
	}
	esc.client = es
	_, err = esc.client.Ping()
	if err != nil {
		return err
	}
	return nil
}

func (esc *EslasticClient[MsgType]) Search(ctx context.Context, index string, query string, resultQueue chan MsgType, logParser func(InnerHit) MsgType) error {
	defer func() {
		// mark result queue done
		close(resultQueue)
	}()

	esClient := esc.client

	res, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return err
	}
	parsedResp, err := parseElkResp(res)
	if err != nil {
		return err
	}

	// parse the hits and push to the result queue
	// log.Printf("got total hits: %d", len(parsedResp.Hits.Hits))
	for _, hit := range parsedResp.Hits.Hits {
		// log.Printf("hit: %v", hit)
		resultQueue <- logParser(hit)
	}
	return nil
}

func (esc *EslasticClient[MsgType]) SearchScroll(ctx context.Context, index string, query string, resultQueue chan MsgType, logParser func(InnerHit) MsgType) error {
	// Search with a scroll
	defer func() {
		// mark result queue done
		close(resultQueue)
	}()

	esClient := esc.client
	var scrollID string

	res, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(strings.NewReader(query)),
		esClient.Search.WithScroll(elkScrollTime),
	)
	if err != nil {
		return err
	}
	parsedResp, err := parseElkResp(res)
	if err != nil {
		return err
	}

	scrollID = parsedResp.ScrollID
	// parse the hits and push to the result queue
	// log.Printf("got total hits: %d", len(parsedResp.Hits.Hits))
	for _, hit := range parsedResp.Hits.Hits {
		resultQueue <- logParser(hit)
	}

	// Iterate over the hits
	for {
		res, err := esClient.Scroll(
			esClient.Scroll.WithContext(ctx),
			esClient.Scroll.WithScrollID(scrollID),
			esClient.Scroll.WithScroll(elkScrollTime),
		)
		if err != nil {
			log.Warn().Err(err).Msg("scroll get error")
			return err
		}

		parsedResp, err := parseElkResp(res)
		if err != nil {
			log.Warn().Err(err).Msg("error parse elk response")
			return err
		}
		// exit if no more hits
		if len(parsedResp.Hits.Hits) == 0 {
			break
		}
		scrollID = parsedResp.ScrollID
		// parse the hits and push to the result queue
		// log.Printf("got total hits: %d", len(parsedResp.Hits.Hits))
		for _, hit := range parsedResp.Hits.Hits {
			resultQueue <- logParser(hit)
		}
	}
	// clear the scroll
	go esc.clearScroll(scrollID)
	return nil
}
