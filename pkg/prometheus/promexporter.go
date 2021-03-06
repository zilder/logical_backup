package promexporter

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricsKind uint8

const (
	prometheusNamespace = "lbt"

	MessageCounter                   = "backup_messages_total"
	FilesArchivedCounter             = "backup_files_archived_total"
	FilesArchivedTimeoutCounter      = "backup_files_timeout_archived_total"
	TransactionCounter               = "backup_transactions_total"
	TotalBytesWrittenCounter         = "backup_bytes_written_total"
	FlushLSNCGauge                   = "backup_flush_lsn_counter"
	LastCommitTimestampGauge         = "backup_last_commit_timestamp"
	LastWrittenMessageTimestampGauge = "backup_last_written_message_timestamp"

	PerTableMessageCounter              = "backup_messages_per_table"
	PerTableBytesCounter                = "backup_bytes_per_table"
	PerTablesFilesArchivedCounter       = "backup_files_archived_per_table"
	PerTableFilesArchivedTimeoutCounter = "backup_files_timeout_archived_per_table"
	PerTableLastCommitTimestampGauge    = "backup_last_commit_timestamp_per_table"
	PerTableLastBackupEndTimestamp      = "backup_last_backup_end_timestamp_per_table"
	PerTableMessageSinceLastBackupGauge = "backup_messages_since_last_basebackup_per_table"

	MessageTypeLabel = "message_type"
	TableNameLabel   = "table_name"
	TableOIDLabel    = "table_oid"

	MetricsCounter MetricsKind = iota
	MetricsCounterVector
	MetricsGauge
	MetricsGaugeVector
)

type MetricsToRegister struct {
	Name   string
	Help   string
	Labels []string
	Kind   MetricsKind
}

type PrometheusExporter struct {
	metrics map[string]interface{}
	port    int
}

type PromInterface interface {
	RegisterMetricsItem(item *MetricsToRegister) error
	Inc(name string, labelValues []string) error
	Add(name string, addition float64, labelValues []string) error
	Set(name string, value float64, labelValues []string) error
	Reset(name string, labelValues []string) error
	SetToCurrentTime(name string, labelValues []string) error
	Run(ctx context.Context, wait *sync.WaitGroup)
}

func New(port int) *PrometheusExporter {
	return &PrometheusExporter{metrics: make(map[string]interface{}), port: port}
}

func (pe *PrometheusExporter) errorIfExists(name, typeName string) error {
	_, ok := pe.metrics[name]
	if ok {
		return fmt.Errorf("%s with the name %q is already registered", typeName, name)
	}

	return nil
}

func (pe *PrometheusExporter) RegisterMetricsItem(item *MetricsToRegister) error {
	switch item.Kind {
	case MetricsCounter:
		return pe.registerCounter(item.Name, item.Help)
	case MetricsCounterVector:
		return pe.registerCounterVector(item.Name, item.Help, item.Labels)
	case MetricsGauge:
		return pe.registerGauge(item.Name, item.Help)
	case MetricsGaugeVector:
		return pe.registerGaugeVector(item.Name, item.Help, item.Labels)
	default:
		return fmt.Errorf("unknonw metrics type code to register: %d for item %q", item.Kind, item.Name)
	}
}

func (pe *PrometheusExporter) registerCounterVector(name, help string, labelNames []string) error {
	if err := pe.errorIfExists(name, "counter vector"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewCounterVec(prom.CounterOpts{Name: name, Namespace: prometheusNamespace, Help: help},
		labelNames)

	return nil
}

func (pe *PrometheusExporter) registerCounter(name, help string) error {
	if err := pe.errorIfExists(name, "counter"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewCounter(prom.CounterOpts{Name: name, Namespace: prometheusNamespace, Help: help})

	return nil
}

func (pe *PrometheusExporter) registerGauge(name, help string) error {
	if err := pe.errorIfExists(name, "counter"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewGauge(prom.GaugeOpts{Name: name, Namespace: prometheusNamespace, Help: help})

	return nil
}

func (pe *PrometheusExporter) registerGaugeVector(name, help string, labelNames []string) error {
	if err := pe.errorIfExists(name, "gauge vector"); err != nil {
		return err
	}

	pe.metrics[name] = promauto.NewGaugeVec(prom.GaugeOpts{Name: name, Namespace: prometheusNamespace, Help: help},
		labelNames)

	return nil
}

func (pe *PrometheusExporter) Inc(name string, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Counter:
		t.Inc()
	case prom.Gauge:
		t.Inc()
	case *prom.CounterVec:
		t.WithLabelValues(labelValues...).Inc()
	case *prom.GaugeVec:
		t.WithLabelValues(labelValues...).Inc()
	default:
		return fmt.Errorf("type %T doesn't support Inc", t)
	}

	return nil
}

func (pe *PrometheusExporter) Add(name string, addition float64, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Counter:
		t.Add(addition)
	case *prom.CounterVec:
		t.WithLabelValues(labelValues...).Add(addition)
	default:
		return fmt.Errorf("type %T doesn't support Add", t)
	}

	return nil
}

func (pe *PrometheusExporter) Set(name string, value float64, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Gauge:
		t.Set(value)
	case *prom.GaugeVec:
		t.WithLabelValues(labelValues...).Set(value)
	default:
		return fmt.Errorf("type %T doesn't support Set", t)
	}

	return nil
}

func (pe *PrometheusExporter) Reset(name string, labelValues []string) error {
	return pe.Set(name, 0, labelValues)
}

func (pe *PrometheusExporter) SetToCurrentTime(name string, labelValues []string) error {
	switch t := pe.metrics[name].(type) {
	case prom.Gauge:
		t.SetToCurrentTime()
	case prom.GaugeVec:
		t.WithLabelValues(labelValues...).SetToCurrentTime()
	default:
		return fmt.Errorf("type %T doesn't support SetToCurrentTime", t)
	}

	return nil
}

func (pe *PrometheusExporter) Run(ctx context.Context, wait *sync.WaitGroup) {
	defer wait.Done()

	srv := &http.Server{Addr: fmt.Sprintf(":%d", pe.port)}

	wait.Add(1)
	go func() {
		defer wait.Done()
		<-ctx.Done()
		if err := srv.Close(); err != nil {
			log.Printf("error while closing prometheus connections: %v", err)
		}

		log.Printf("prometheus http server shut down")
	}()

	// TODO: avoid exposting noisy metrics about the prometheus itself
	http.Handle("/", http.RedirectHandler("/metrics", http.StatusMovedPermanently))
	http.Handle("/metrics", promhttp.Handler())

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("prometheus exporter routine closed with error %v", err)
	}
}
