package ceph

import (
	"fmt"
	"testing"

	"github.com/opensds/opensds/pkg/model"
)

func TestMetricDriverSetup(t *testing.T) {
	var d = &MetricDriver{}

	if err := d.Setup(); err != nil {
		t.Errorf("Setup ceph metric  driver failed: %+v\n", err)
	}

}

func TestCollectMetrics(t *testing.T) {

	//metricList := []string{"osd_perf_commit_latency", "osd_perf_apply_latency", "cluster_capacity_bytes", "pool_write_total", "pool_read_bytes_total"}
	var metricDriver = &MetricDriver{}
	metricDriver.Setup()
	metricArray, err := metricDriver.CollectMetrics()
	if err != nil {
		t.Errorf("CollectMetrics call to ceph driver failed: %+v\n", err)
	}
	metricDriver.Teardown()
	//for _, m := range metricArray {
	//	t.Log("Metric Values \n",*(m.MetricValues[0]))
	//}
	//for i:=0; i< len(metricList);i++{
	//	fmt.Println("Metric array \n",metricArray[i])
	//}

	printMetricSpec(metricArray)

}
func printMetricSpec(m []*model.MetricSpec) {
	for _, p := range m {
		fmt.Printf("%+v\n", p)
		for _, v := range p.MetricValues {
			fmt.Printf("%+v\n", v)
		}
	}

}
