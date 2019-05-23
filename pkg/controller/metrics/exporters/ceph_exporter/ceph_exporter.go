package main

import (
	"fmt"

	log "github.com/golang/glog"
	"github.com/opensds/opensds/contrib/drivers/ceph"

	//"github.com/opensds/opensds/contrib/drivers/ceph"

	//"github.com/opensds/opensds/contrib/drivers/ceph"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v2"
)

type cephcollector struct {
	mu                       sync.Mutex
	pool_used_bytes          *prometheus.Desc
	pool_raw_used_bytes      *prometheus.Desc
	pool_available_bytes     *prometheus.Desc
	pool_objects_total       *prometheus.Desc
	pool_dirty_objects_total *prometheus.Desc
	pool_read_total          *prometheus.Desc
	pool_read_bytes_total    *prometheus.Desc
	pool_write_total         *prometheus.Desc
	pool_write_bytes_total   *prometheus.Desc
}

func newCephCollector() *cephcollector {
	//metricDriver := ceph.MetricDriver{}
	//metricDriver.Setup()
	//var list []string

	//list =	append(list, "pool_used_bytes")
	//metricArray, _ := metricDriver.CollectMetrics(list,"pool")
	//for k:= range metricArray{
	//	type ceph_collect struct {
	//		mu sync.Mutex
	//
	//	}
	//}

	var labelKeys = []string{"pool"}
	var const_label = make(map[string]string)
	const_label["cluster"] = "ceph"

	//constlable:= make(map[string]string)
	//constlable= "g"
	fmt.Println("newCephCollector method")
	return &cephcollector{

		pool_used_bytes: prometheus.NewDesc("ceph_pool_used_bytes",
			"Capacity of the pool that is currently under use",
			labelKeys, const_label,
		),
		pool_raw_used_bytes: prometheus.NewDesc("ceph_pool_raw_used_bytes",
			"Raw capacity of the pool that is currently under use, this factors in the size",
			labelKeys, const_label,
		),
		pool_available_bytes: prometheus.NewDesc("ceph_pool_available_bytes",
			"Free space for this ceph pool",
			labelKeys, const_label,
		),
		pool_objects_total: prometheus.NewDesc("ceph_pool_objects_total",
			"Total no. of objects allocated within the pool",
			labelKeys, const_label,
		),
		pool_dirty_objects_total: prometheus.NewDesc("ceph_pool_dirty_objects_total",
			"Total no. of dirty objects in a cache-tier pool",
			labelKeys, const_label,
		),
		pool_read_total: prometheus.NewDesc("ceph_pool_read_total",
			"Total read i/o calls for the pool",
			labelKeys, const_label,
		),
		pool_read_bytes_total: prometheus.NewDesc("ceph_pool_read_bytes_total",
			"Total read throughput for the pool",
			labelKeys, const_label,
		),
		pool_write_total: prometheus.NewDesc("ceph_pool_write_total",
			"Total write i/o calls for the pool",
			labelKeys, const_label,
		),
		pool_write_bytes_total: prometheus.NewDesc("ceph_pool_write_bytes_total",
			"Total write throughput for the pool",
			labelKeys, const_label,
		),
	}
}

func (c *cephcollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pool_used_bytes
	ch <- c.pool_raw_used_bytes
	ch <- c.pool_available_bytes
	ch <- c.pool_objects_total
	ch <- c.pool_dirty_objects_total
	ch <- c.pool_read_total
	ch <- c.pool_read_bytes_total
	ch <- c.pool_write_total
	ch <- c.pool_write_bytes_total
}

type Config struct {
	Type    string   `type`
	Devices []string `devices`
}

type Configs struct {
	Cfgs []*Config `resources`
}

func (c *cephcollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("Collect method")
	//Implement logic here to determine proper metric value to return to prometheus
	//for each descriptor
	//metricList:=[]string{"pool_available_bytes"}
	metricList := []string{"pool_used_bytes", "pool_raw_used_bytes", "pool_available_bytes", "pool_objects_total", "pool_dirty_objects_total", "pool_read_total", "pool_read_bytes_total", "pool_write_total", "pool_write_bytes_total"}
	source, err := ioutil.ReadFile("/home/demo/go/src/github.com/opensds/opensds/pkg/controller/metrics/exporters/ceph_exporter/resources.yaml")
	if err != nil {
		log.Fatal("config file can't read", err)
	}
	var config Configs
	err1 := yaml.Unmarshal(source, &config)
	if err1 != nil {
		log.Fatalf("error: %v", err)
	}

	metricDriver := ceph.MetricDriver{}
	metricDriver.Setup()
	for _, resource := range config.Cfgs {
		switch resource.Type {
		case "pool":
			for _, volume := range resource.Devices {
				fmt.Println("volume", volume)
				metricArray, _ := metricDriver.CollectMetrics(metricList, volume)
				for _, m := range metricArray {

					fmt.Printf("Metric Values %v\n", *(m.MetricValues[0]))
				}

				for _, metric := range metricArray {
					var metric_name string
					//var lableVals []string
					//for k := range metric.Labels{
					//	lableVals=append(lableVals,metric.Labels[k] )
					//}

					lableVals := []string{metric.Labels["pool"]}
					if metric.AggrType != "" {
						if metric.Unit != "" {
							metric_name = fmt.Sprintf("%s_%s_%s_%s", metric.Component, metric.Name, metric.Unit, metric.AggrType)

						} else {
							metric_name = fmt.Sprintf("%s_%s_%s", metric.Component, metric.Name, metric.AggrType)
						}

					} else {
						if metric.Unit != "" {
							metric_name = fmt.Sprintf("%s_%s_%s", metric.Component, metric.Name, metric.Unit)
						} else {
							metric_name = fmt.Sprintf("%s_%s", metric.Component, metric.Name)
						}

					}

					switch metric_name {
					case "pool_used_bytes":
						ch <- prometheus.MustNewConstMetric(c.pool_used_bytes, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_raw_used_bytes":
						ch <- prometheus.MustNewConstMetric(c.pool_raw_used_bytes, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_available_bytes":
						ch <- prometheus.MustNewConstMetric(c.pool_available_bytes, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_objects_total":
						ch <- prometheus.MustNewConstMetric(c.pool_objects_total, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_dirty_objects_total":
						ch <- prometheus.MustNewConstMetric(c.pool_dirty_objects_total, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_read_total":
						ch <- prometheus.MustNewConstMetric(c.pool_read_total, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_read_bytes_total":
						ch <- prometheus.MustNewConstMetric(c.pool_read_bytes_total, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_write_total":
						ch <- prometheus.MustNewConstMetric(c.pool_write_total, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					case "pool_write_bytes_total":
						ch <- prometheus.MustNewConstMetric(c.pool_write_bytes_total, prometheus.GaugeValue, metric.MetricValues[0].Value, lableVals...)
					}

				}
			}

		}

	}

}
func validateCliArg(arg1 string) string {
	num, err := strconv.Atoi(arg1)
	if (err != nil) || (num > 65535) {

		fmt.Println("please enter a valid port number")
		os.Exit(1)
	}
	return arg1
}

func main() {
	//portNo := validateCliArg(os.Args[1])
	//Create a new instance of the lvmcollector and
	//register it with the prometheus client.
	ceph := newCephCollector()
	prometheus.MustRegister(ceph)

	//This section will start the HTTP server and expose
	//any metrics on the /metrics endpoint.
	http.Handle("/metrics", promhttp.Handler())
	//log.Info("lvm exporter begining to serve on port :" + portNo)
	log.Fatal(http.ListenAndServe("192.168.0.192:9121", nil))
}
