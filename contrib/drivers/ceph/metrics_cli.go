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

type cephHealthStats struct {
	Health struct {
		Summary []struct {
			Severity string `json:"severity"`
			Summary  string `json:"summary"`
		} `json:"summary"`
		OverallStatus string `json:"overall_status"`
		Status        string `json:"status"`
		Checks        map[string]struct {
			Severity string `json:"severity"`
			Summary  struct {
				Message string `json:"message"`
			} `json:"summary"`
		} `json:"checks"`
	} `json:"health"`
	OSDMap struct {
		OSDMap struct {
			NumOSDs        float64 `json:"num_osds"`
			NumUpOSDs      float64 `json:"num_up_osds"`
			NumInOSDs      float64 `json:"num_in_osds"`
			NumRemappedPGs float64 `json:"num_remapped_pgs"`
		} `json:"osdmap"`
	} `json:"osdmap"`
	PGMap struct {
		NumPGs                  float64 `json:"num_pgs"`
		WriteOpPerSec           float64 `json:"write_op_per_sec"`
		ReadOpPerSec            float64 `json:"read_op_per_sec"`
		WriteBytePerSec         float64 `json:"write_bytes_sec"`
		ReadBytePerSec          float64 `json:"read_bytes_sec"`
		RecoveringObjectsPerSec float64 `json:"recovering_objects_per_sec"`
		RecoveringBytePerSec    float64 `json:"recovering_bytes_per_sec"`
		RecoveringKeysPerSec    float64 `json:"recovering_keys_per_sec"`
		CacheFlushBytePerSec    float64 `json:"flush_bytes_sec"`
		CacheEvictBytePerSec    float64 `json:"evict_bytes_sec"`
		CachePromoteOpPerSec    float64 `json:"promote_op_per_sec"`
		DegradedObjects         float64 `json:"degraded_objects"`
		MisplacedObjects        float64 `json:"misplaced_objects"`
		PGsByState              []struct {
			Count  float64 `json:"count"`
			States string  `json:"state_name"`
		} `json:"pgs_by_state"`
	} `json:"pgmap"`
}

func (cli *MetricCli) CollectMetrics(metricList []string, instanceID string, resourceType string) ([]CephMetricStats, error) {

	returnMap := []CephMetricStats{}
	var err error
	conn, err := rados.NewConn()
	if err != nil {
		log.Error("when connecting to rados:", err)
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		log.Error("file ReadDefaultConfigFile can't read", err)
	}

	err = conn.Connect()
	if err != nil {
		log.Error("when connecting to ceph cluster:", err)
	}

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
		buf, _, err := conn.MonCommand(cmd)
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
		buf, _, err := conn.MonCommand(cmd)
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
		buf, _, err := conn.MonCommand(cmd)
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
	conn.Shutdown()
	return returnMap, nil
}
