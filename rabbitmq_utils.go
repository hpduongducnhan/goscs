package goscs

import (
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// getString extracts a string value from amqp.Table, like Python's dict.get()
func rbmqGetString(table amqp091.Table, key string) string {
	if val, ok := table[key].(string); ok {
		return val
	}
	return ""
}

// getInt extracts an int value from amqp.Table
func rbmqGetInt(table amqp091.Table, key string) int {
	// fmt.Printf("rbmqGetInt key %s type %T ", key, table[key])
	if val, ok := table[key].(int32); ok { // RabbitMQ returns int as int32
		return int(val)
	}
	if val, ok := table[key].(int64); ok { // RabbitMQ returns int as int64
		return int(val)
	}
	if val, ok := table[key].(int); ok { // Normal int type
		return val
	}
	return 0
}

// getSlice extracts a []string from amqp.Table
func rbmqGetSliceString(table amqp091.Table, key string) []string {
	if val, ok := table[key].([]interface{}); ok {
		strSlice := make([]string, len(val))
		for i, v := range val {
			strSlice[i], _ = v.(string) // Convert each value to string
		}
		return strSlice
	}
	return make([]string, 0)
}

// getTime extracts a time.Time value from amqp.Table, like Python's dict.get()
func rbmqGetTime(table amqp091.Table, key string) time.Time {
	if val, ok := table[key].(time.Time); ok {
		return val
	}
	return time.Time{}
}
