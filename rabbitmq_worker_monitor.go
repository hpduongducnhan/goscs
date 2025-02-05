package goscs

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shirou/gopsutil/process"
)

// Get worker's PID
var workerPID = os.Getpid()

func collectWorkerMetrics(
	workerCPUUsage, workerMemUsage, workerIOReadBytes, workerIOWriteBytes prometheus.Gauge,
) {
	proc, err := process.NewProcess(int32(workerPID))
	if err != nil {
		log.Fatalf("Failed to get worker process: %v", err)
	}

	for {
		// Get CPU usage
		cpuPercent, err := proc.CPUPercent()
		if err == nil {
			workerCPUUsage.Set(cpuPercent)
		}

		// Get memory usage
		memInfo, err := proc.MemoryInfo()
		if err == nil {
			workerMemUsage.Set(float64(memInfo.RSS)) // Resident Set Size (actual memory usage)
		}

		// Get I/O stats
		ioStat, err := proc.IOCounters()
		if err == nil {
			workerIOReadBytes.Set(float64(ioStat.ReadBytes))
			workerIOWriteBytes.Set(float64(ioStat.WriteBytes))
		}

		// Wait before collecting metrics again
		time.Sleep(5 * time.Second)
	}
}

func (rw *RabbitMQWorker) attachPrometheus() {
	if rw.PrometheusEnable {
		if rw.PrometheusMetricUrl == "" {
			rw.PrometheusMetricUrl = "/metrics"
		}

		// declare prometheus
		messagesProcessed := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "rabbitmq_messages_processed_total",
				Help: "Total number of messages processed from RabbitMQ",
			},
		)

		messageProcessingTime := prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "rabbitmq_message_processing_seconds",
				Help:    "Time taken to process each RabbitMQ message",
				Buckets: prometheus.DefBuckets,
			},
		)

		cpuUsage := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_cpu_usage_percent",
				Help: "Current CPU usage percentage of the worker",
			},
		)

		memUsage := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_memory_usage_bytes",
				Help: "Current memory usage of the worker in bytes",
			},
		)

		ioReadBytes := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_io_read_bytes",
				Help: "Bytes read by the worker",
			},
		)

		ioWriteBytes := prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "worker_io_write_bytes",
				Help: "Bytes written by the worker",
			},
		)

		// collect system info with interval
		go collectWorkerMetrics(cpuUsage, memUsage, ioReadBytes, ioWriteBytes)

		// register
		prometheus.MustRegister(messagesProcessed)
		prometheus.MustRegister(messageProcessingTime)
		prometheus.MustRegister(cpuUsage)
		prometheus.MustRegister(memUsage)
		prometheus.MustRegister(ioReadBytes)
		prometheus.MustRegister(ioWriteBytes)

	}
}

func (rw *RabbitMQWorker) runWithMonitorAgent() {
	if rw.workerRestSvr != nil {
		return
	}
	rw.attachPrometheus()

	mux := http.NewServeMux()
	mux.HandleFunc(rw.WorkerRestSvrHealthCheckUrl, func(w http.ResponseWriter, r *http.Request) {
		response := `{"status":"ok"}`
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, response)
	})
	mux.Handle(rw.PrometheusMetricUrl, promhttp.Handler())
	rw.workerRestSvr = &http.Server{
		Addr:    fmt.Sprintf(":%d", rw.WorekrRestSvrPort),
		Handler: mux,
	}

	// add goroutine if you need
	func() {
		if err := rw.workerRestSvr.ListenAndServe(); err != http.ErrServerClosed {
			rw.Logger.Error().Err(err).Msg("HTTP server error, shutdown")
		}
	}()
}
