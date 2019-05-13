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
	"encoding/json"
	"fmt"
	"github.com/ceph/go-ceph/rados"
	log "github.com/golang/glog"
)

type MetricCli struct {
	conn *rados.Conn
}

type CephMetricStats struct {
	Name        string
	Value       string
	Unit        string
	Const_Label string
	AggrType    string
	Var_Label   string
}

type cephPoolStats struct {
	Pools []struct {
		Name  string `json:"name"`
		ID    int    `json:"id"`
		Stats struct {
			BytesUsed    json.Number `json:"bytes_used"`
			RawBytesUsed json.Number `json:"raw_bytes_used"`
			MaxAvail     json.Number `json:"max_avail"`
			Objects      json.Number `json:"objects"`
			DirtyObjects json.Number `json:"dirty"`
			ReadIO       json.Number `json:"rd"`
			ReadBytes    json.Number `json:"rd_bytes"`
			WriteIO      json.Number `json:"wr"`
			WriteBytes   json.Number `json:"wr_bytes"`
		} `json:"stats"`
	} `json:"pools"`
}

type cephClusterStats struct {
	Stats struct {
		TotalBytes      json.Number `json:"total_bytes"`
		TotalUsedBytes  json.Number `json:"total_used_bytes"`
		TotalAvailBytes json.Number `json:"total_avail_bytes"`
		TotalObjects    json.Number `json:"total_objects"`
	} `json:"stats"`
}

type cephPerfStat struct {
	PerfInfo []struct {
		ID    json.Number `json:"id"`
		Stats struct {
			CommitLatency json.Number `json:"commit_latency_ms"`
			ApplyLatency  json.Number `json:"apply_latency_ms"`
		} `json:"perf_stats"`
	} `json:"osd_perf_infos"`
}

func NewMetricCli() (*MetricCli, error) {

	//var err error
	conn, err := rados.NewConn()
	if err != nil {
		log.Error("when connecting to rados:", err)
		return nil, err
	}
	//
	err = conn.ReadDefaultConfigFile()
	if err != nil {
		log.Error("file ReadDefaultConfigFile can't read", err)
		return nil, err
	}

	err = conn.Connect()
	if err != nil {
		log.Error("when connecting to ceph cluster:", err)
		return nil, err
	}

	return &MetricCli{
		conn: conn,
	}, nil
}

func (cli *MetricCli) CollectMetrics(metricList []string, instanceID string, resourceType string) ([]CephMetricStats, error) {

	returnMap := []CephMetricStats{}
	switch resourceType {
	case "pool":
		cmd, err := json.Marshal(map[string]interface{}{
			"prefix": "df",
			"detail": "detail",
			"format": "json",
		})
		if err != nil {
			// panic! because ideally in no world this hard-coded input
			// should fail.
			log.Errorf("cmd failed with %s\n", err)
		}
		buf, _, err := cli.conn.MonCommand(cmd)
		pool_stats := &cephPoolStats{}
		if err := json.Unmarshal(buf, pool_stats); err != nil {
			log.Fatalf("Unmarshal error: %v", err)
			// return
		}

		for _, pool := range pool_stats.Pools {

			for _, element := range metricList {
				switch element {
				case "pool_used_bytes":
					returnMap = append(returnMap, CephMetricStats{
						"used",
						pool.Stats.BytesUsed.String(),
						"bytes", "ceph",
						"",
						pool.Name})

				case "pool_raw_used_bytes":
					returnMap = append(returnMap, CephMetricStats{
						"raw_used",
						pool.Stats.RawBytesUsed.String(),
						"bytes", "ceph",
						"",
						pool.Name})

				case "pool_available_bytes":
					returnMap = append(returnMap, CephMetricStats{
						"available",
						pool.Stats.MaxAvail.String(),
						"bytes",
						"ceph",
						"",
						pool.Name})

				case "pool_objects_total":
					returnMap = append(returnMap, CephMetricStats{
						"objects",
						pool.Stats.Objects.String(),
						"",
						"ceph",
						"",
						pool.Name})

				case "pool_dirty_objects_total":
					returnMap = append(returnMap, CephMetricStats{
						"dirty_objects",
						pool.Stats.DirtyObjects.String(),
						"",
						"ceph",
						"total",
						pool.Name})

				case "pool_read_total":
					returnMap = append(returnMap, CephMetricStats{
						"read", pool.Stats.ReadIO.String(),
						"",
						"ceph",
						"total",
						pool.Name})

				case "pool_read_bytes_total":
					returnMap = append(returnMap, CephMetricStats{
						"read",
						pool.Stats.ReadBytes.String(),
						"bytes",
						"ceph",
						"total",
						pool.Name})

				case "pool_write_total":
					returnMap = append(returnMap, CephMetricStats{
						"write",
						pool.Stats.WriteIO.String(),
						"", "ceph",
						"",
						pool.Name})

				case "pool_write_bytes_total":
					returnMap = append(returnMap, CephMetricStats{
						"write_bytes",
						pool.Stats.WriteBytes.String(),
						"bytes",
						"ceph",
						"total",
						pool.Name})
				}
			}
		}

	case "cluster":
		cmd, err := json.Marshal(map[string]interface{}{
			"prefix": "df",
			"detail": "detail",
			"format": "json",
		})
		if err != nil {
			// panic! because ideally in no world this hard-coded input
			// should fail.
			log.Errorf("cmd failed with %s\n", err)
		}
		buf, _, err := cli.conn.MonCommand(cmd)
		cluster_stats := &cephClusterStats{}
		if err := json.Unmarshal(buf, cluster_stats); err != nil {

			log.Fatalf("Unmarshal error: %v", err)
			// return
		}

		for _, metric := range metricList {
			switch metric {
			case "cluster_capacity_bytes":
				returnMap = append(returnMap, CephMetricStats{
					"capacity",
					cluster_stats.Stats.TotalBytes.String(),
					"bytes",
					"ceph",
					"",
					""})
			case "cluster_available_bytes":
				returnMap = append(returnMap, CephMetricStats{
					"available",
					cluster_stats.Stats.TotalAvailBytes.String(),
					"bytes",
					"ceph",
					"",
					""})
			case "cluster_used_bytes":
				returnMap = append(returnMap, CephMetricStats{
					"used",
					cluster_stats.Stats.TotalUsedBytes.String(),
					"bytes",
					"ceph",
					"",
					""})
			case "cluster_objects":
				returnMap = append(returnMap, CephMetricStats{
					"objects",
					cluster_stats.Stats.TotalObjects.String(),
					"",
					"ceph",
					"",
					""})

			}
		}

	case "osd":
		cmd, err := json.Marshal(map[string]interface{}{
			"prefix": "osd perf",
			"format": "json",
		})
		if err != nil {
			log.Errorf("cmd failed with %s\n", err)
		}
		buf, _, err := cli.conn.MonCommand(cmd)
		if err != nil {
			log.Errorf("unable to collect data from ceph osd perf")
		}
		osdPerf := &cephPerfStat{}
		if err := json.Unmarshal(buf, osdPerf); err != nil {
			log.Errorf("unmarshal failed")
		}
		for _, perfStat := range osdPerf.PerfInfo {
			osdID, err := perfStat.ID.Int64()
			if err != nil {
				log.Errorf("when collecting ceph cluster metrics")
			}
			osdName := fmt.Sprintf("osd.%v", osdID)

			for _, metric := range metricList {
				switch metric {

				case "osd_perf_commit_latency":
					returnMap = append(returnMap, CephMetricStats{
						"perf_commit_latency",
						perfStat.Stats.CommitLatency.String(),
						"ms",
						"ceph",
						"",
						osdName})
				case "osd_perf_apply_latency":
					returnMap = append(returnMap, CephMetricStats{
						"perf_apply_latency",
						perfStat.Stats.ApplyLatency.String(),
						"ms",
						"ceph",
						"",
						osdName})
				}
			}
		}

	}
	return returnMap, nil
}
