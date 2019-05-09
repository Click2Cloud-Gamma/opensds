package ceph

import "testing"

func TestMetricDriverSetup(t *testing.T) {
	var d = &MetricDriver{}

	if err := d.Setup(); err != nil {
		t.Errorf("Setup ceph metric  driver failed: %+v\n", err)
	}

}

func TestCollectMetrics(t *testing.T) {

	metricList := []string{"TotalBytes", "NumOSDs", "ReadOpPerSec", "WriteBytePerSec", "ReadBytePerSec", "RecoveringObjectsPerSec", "RecoveringBytePerSec"}
	//metricList :=[] string{"TotalBytes"}
	var metricDriver = &MetricDriver{}
	metricDriver.Setup()
	metricArray, err := metricDriver.CollectMetrics(metricList, "ceph_health_status", "health")
	if err != nil {
		t.Errorf("collectMetrics call to ceph driver failed: %+v\n", err)
	}

	for _, m := range metricArray {
		t.Log(*m)
	}
	for _, m := range metricArray {
		t.Log("Metric Values \n", *(m.MetricValues[0]))
	}

}
