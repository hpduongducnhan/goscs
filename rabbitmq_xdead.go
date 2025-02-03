package goscs

import (
	"fmt"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type RbmqXDeadInfo struct {
	Success        bool
	Count          int
	Reason         string
	Exchange       string
	Queue          string
	DeadQueue      string
	Time           time.Time
	RoutingKey     string
	RoutingKeyDead string
}

func (r *RbmqXDeadInfo) Validate() {
	if r.Count > 0 && r.Exchange != "" && r.Queue != "" && r.RoutingKey != "" {
		// create dead queue name
		lastDelay := strings.LastIndex(r.Queue, "-delay")
		if lastDelay != -1 {
			r.DeadQueue = r.Queue[:lastDelay] + "-dead"
		}

		lastRetry := strings.LastIndex(r.RoutingKey, ".retry")
		if lastRetry != -1 {
			r.RoutingKeyDead = r.RoutingKey[:lastRetry] + ".dead"
		}
		r.Success = true
	} else {
		r.Success = false
	}
}

func (r *RbmqXDeadInfo) ParseXDead(headers amqp091.Table) {
	xDeath, ok := headers["x-death"].([]interface{})
	if !ok {
		fmt.Println("No x-death header found")
		return
	}
	for _, entry := range xDeath {
		deathInfo, ok := entry.(amqp091.Table)
		if !ok {
			continue
		}
		routingKeys := rbmqGetSliceString(deathInfo, "routing-keys")
		for _, key := range routingKeys {
			if strings.HasSuffix(key, ".retry") {
				fmt.Printf("--> deathInfo %+v\n", deathInfo)
				r.RoutingKey = key
				r.Count = rbmqGetInt(deathInfo, "count")
				r.Reason = rbmqGetString(deathInfo, "reason")
				r.Exchange = rbmqGetString(deathInfo, "exchange")
				r.Queue = rbmqGetString(deathInfo, "queue")
				r.Time = rbmqGetTime(deathInfo, "time")
				r.Validate()
				return
			}
		}
	}
	r.Validate()
}
