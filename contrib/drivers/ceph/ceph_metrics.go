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
	"strconv"
	"time"

	"github.com/opensds/opensds/pkg/model"
)

// TODO: Move this Yaml config to a file
var data = `
resources:
  - resource: pool
    metrics:
      - pool_used_bytes
      - pool_raw_used_bytes
      - pool_available_bytes
      - pool_objects_total
      - pool_dirty_objects_total
      - pool_read_total
      - pool_read_bytes_total
      - pool_write_total
      - pool_write_bytes_total
  - resource: cluster
    metrics:
      - cluster_capacity_bytes
      - cluster_used_bytes
      - cluster_available_bytes
      - cluster_objects
  - resource: osd
    metrics:
      - perf_commit_latency
      - perf_apply_latency
      - crush_weight
      - depth
      - reweight
      - bytes
      - used_bytes
      - avail_bytes
      - utilization
      - variance
      - pgs
      - total_bytes
      - total_used_bytes
      - total_avail_bytes
      - average_utilization
`

type MetricDriver struct {
	cli *MetricCli
}

func getCurrentUnixTimestamp() int64 {
	now := time.Now()
	secs := now.Unix()
	return secs
}

func (d *MetricDriver) CollectMetrics() ([]*model.MetricSpec, error) {

	metricMap, err := d.cli.CollectMetrics()

	var tempMetricArray []*model.MetricSpec

	for i := 0; i < len(metricMap); i++ {
		val, _ := strconv.ParseFloat(metricMap[i].Value, 64)
		//Todo: See if association  is required here, resource discovery could fill this information
		associatorMap := make(map[string]string)
		for k := range metricMap[i].Const_Label {
			associatorMap[k] = metricMap[i].Const_Label[k]
		}
		if metricMap[i].Var_Label != nil {
			for k := range metricMap[i].Var_Label {
				associatorMap[k] = metricMap[i].Var_Label[k]
			}
		}
		metricValue := &model.Metric{
			Value:     val,
			Timestamp: getCurrentUnixTimestamp(),
		}
		metricValues := make([]*model.Metric, 0)
		metricValues = append(metricValues, metricValue)

		metric := &model.MetricSpec{
			InstanceID:   "ceph_cluster",
			InstanceName: "001",
			Job:          "ceph",
			Labels:       associatorMap,
			//Todo Take Componet from Post call, as of now it is only for pool ( will use "resourceType" instead
			// Pass "resourceType" as 3rd parameter which will be used as Componet's field
			Component:    metricMap[i].Component,
			Name:         metricMap[i].Name,
			Unit:         metricMap[i].Unit,
			AggrType:     metricMap[i].AggrType,
			MetricValues: metricValues,
		}
		tempMetricArray = append(tempMetricArray, metric)
	}
	metricArray := tempMetricArray
	return metricArray, err
}

func (d *MetricDriver) Setup() error {
	cli, err := NewMetricCli()
	if err != nil {
		return err
	}
	d.cli = cli
	return nil
}

func (d *MetricDriver) Teardown() error {
	d.cli.conn.Shutdown()
	return nil
}
