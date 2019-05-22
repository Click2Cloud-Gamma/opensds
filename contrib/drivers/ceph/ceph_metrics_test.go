// Copyright (c) 2019 The OpenSDS Authors.
//
//    Licensed under the Apache License, Version 2.0 (the "License"); you may
//    not use this file except in compliance with the License. You may obtain
//    a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//    License for the specific language governing permissions and limitations
//    under the License.
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

var fakeresp map[string]*MetricFakeResp = map[string]*MetricFakeResp{"df": {[]byte(`{"stats":{"total_bytes":494462976000,"total_used_bytes":238116864,"total_avail_bytes":494224859136,"total_objects":14},"pools":[{"name":"rbd","id":1,"stats":{"kb_used":1,"bytes_used":859,"percent_used":0.00,"max_avail":469501706240,"objects":14,"quota_objects":0,"quota_bytes":0,"dirty":14,"rd":145,"rd_bytes":918304,"wr":1057,"wr_bytes":16384,"raw_bytes_used":859}},{"name":"samp_pool","id":2,"stats":{"kb_used":0,"bytes_used":0,"percent_used":0.00,"max_avail":156500574208,"objects":0,"quota_objects":0,"quota_bytes":0,"dirty":0,"rd":0,"rd_bytes":0,"wr":0,"wr_bytes":0,"raw_bytes_used":0}},{"name":"pool3","id":3,"stats":{"kb_used":0,"bytes_used":0,"percent_used":0.00,"max_avail":469501706240,"objects":0,"quota_objects":0,"quota_bytes":0,"dirty":0,"rd":0,"rd_bytes":0,"wr":0,"wr_bytes":0,"raw_bytes_used":0}}]}`), "", nil}}

type MetricFakeconn struct {
	RespMap map[string]*MetricFakeResp
}

func NewMetricFakeconn(respMap map[string]*MetricFakeResp) Conn {
	return &MetricFakeconn{RespMap: fakeresp}
}

type MetricFakeResp struct {
	out []byte
	sam string
	err error
}

func (n *MetricFakeconn) ReadDefaultConfigFile() error {
	return nil
}

func (n *MetricFakeconn) Connect() error {
	return nil
}

func (n *MetricFakeconn) MonCommand(_ []byte) ([]byte, string, error) {
	buffer := fakeresp["df"].out
	return buffer, "", nil
}

func (n *MetricFakeconn) Shutdown() {}

func TestCollectMetrics(t *testing.T) {
	var md = &MetricDriver{}
	md.Setup()
	md.cli.con = NewMetricFakeconn(fakeresp)
	metricList := []string{"osd_perf_commit_latency", "osd_perf_apply_latency", "cluster_capacity_bytes", "pool_write_total", "pool_read_bytes_total"}

	//md.cli.CollectMetrics(metricList,"pool")
	metricArray, err := md.CollectMetrics(metricList, "pool")
	if err != nil {
		t.Errorf("CollectMetrics call to ceph driver failed: %+v\n", err)
	}
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
