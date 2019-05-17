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

func NewMetricCli() (*MetricCli, error) {

	conn, err := rados.NewConn()
	if err != nil {
		log.Error("when connecting to rados:", err)
		return nil, err
	}

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
		conn,
	}, nil
}

type CephMetricStats struct {
	Name        string
	Value       string
	Unit        string
	Const_Label map[string]string
	AggrType    string
	Var_Label   map[string]string
	Help        string
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

type cephOSDDF struct {
	OSDNodes []struct {
		Name        json.Number `json:"name"`
		CrushWeight json.Number `json:"crush_weight"`
		Depth       json.Number `json:"depth"`
		Reweight    json.Number `json:"reweight"`
		KB          json.Number `json:"kb"`
		UsedKB      json.Number `json:"kb_used"`
		AvailKB     json.Number `json:"kb_avail"`
		Utilization json.Number `json:"utilization"`
		Variance    json.Number `json:"var"`
		Pgs         json.Number `json:"pgs"`
	} `json:"nodes"`

	Summary struct {
		TotalKB      json.Number `json:"total_kb"`
		TotalUsedKB  json.Number `json:"total_kb_used"`
		TotalAvailKB json.Number `json:"total_kb_avail"`
		AverageUtil  json.Number `json:"average_utilization"`
	} `json:"summary"`
}

type cephOSDDump struct {
	OSDs []struct {
		OSD json.Number `json:"osd"`
		Up  json.Number `json:"up"`
		In  json.Number `json:"in"`
	} `json:"osds"`
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
			NumOSDs        json.Number `json:"num_osds"`
			NumUpOSDs      json.Number `json:"num_up_osds"`
			NumInOSDs      json.Number `json:"num_in_osds"`
			NumRemappedPGs json.Number `json:"num_remapped_pgs"`
		} `json:"osdmap"`
	} `json:"osdmap"`
	PGMap struct {
		NumPGs                  json.Number `json:"num_pgs"`
		WriteOpPerSec           json.Number `json:"write_op_per_sec"`
		ReadOpPerSec            json.Number `json:"read_op_per_sec"`
		WriteBytePerSec         json.Number `json:"write_bytes_sec"`
		ReadBytePerSec          json.Number `json:"read_bytes_sec"`
		RecoveringObjectsPerSec json.Number `json:"recovering_objects_per_sec"`
		RecoveringBytePerSec    json.Number `json:"recovering_bytes_per_sec"`
		RecoveringKeysPerSec    json.Number `json:"recovering_keys_per_sec"`
		CacheFlushBytePerSec    json.Number `json:"flush_bytes_sec"`
		CacheEvictBytePerSec    json.Number `json:"evict_bytes_sec"`
		CachePromoteOpPerSec    json.Number `json:"promote_op_per_sec"`
		DegradedObjects         json.Number `json:"degraded_objects"`
		MisplacedObjects        json.Number `json:"misplaced_objects"`
		PGsByState              []struct {
			Count  float64 `json:"count"`
			States string  `json:"state_name"`
		} `json:"pgs_by_state"`
	} `json:"pgmap"`
}

func (cli *MetricCli) CollectPoolMetrics() ([]CephMetricStats, error) {
	returnMap := []CephMetricStats{}
	const_label := make(map[string]string)
	const_label["cluster"] = "ceph"
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "df",
		"detail": "detail",
		"format": "json",
	})
	if err != nil {
		log.Errorf("cmd failed with %s\n", err)
	}

	buf, _, err := cli.conn.MonCommand(cmd)
	if err != nil {
	}

	pool_stats := &cephPoolStats{}
	if err := json.Unmarshal(buf, pool_stats); err != nil {
		log.Errorf("unmarshal error: %v", err)
	}

	for _, pool := range pool_stats.Pools {

		var_label := make(map[string]string)
		var_label["pool"] = pool.Name
		returnMap = append(returnMap, CephMetricStats{
			"used",
			pool.Stats.BytesUsed.String(),
			"bytes", const_label,
			"",
			var_label,
			"Capacity of the pool that is currently under use"})

		returnMap = append(returnMap, CephMetricStats{
			"raw_used",
			pool.Stats.RawBytesUsed.String(),
			"bytes", const_label,
			"",
			var_label,
			"Raw capacity of the pool that is currently under use, this factors in the size"})

		returnMap = append(returnMap, CephMetricStats{
			"available",
			pool.Stats.MaxAvail.String(),
			"bytes",
			const_label,
			"",
			var_label,
			"Free space for this ceph pool"})

		returnMap = append(returnMap, CephMetricStats{
			"objects",
			pool.Stats.Objects.String(),
			"",
			const_label,
			"total",
			var_label,
			"Total no. of objects allocated within the pool"})

		returnMap = append(returnMap, CephMetricStats{
			"dirty_objects",
			pool.Stats.DirtyObjects.String(),
			"",
			const_label,
			"total",
			var_label,
			"Total no. of dirty objects in a cache-tier pool"})

		returnMap = append(returnMap, CephMetricStats{
			"read", pool.Stats.ReadIO.String(),
			"",
			const_label,
			"total",
			var_label, "Total read i/o calls for the pool"})

		returnMap = append(returnMap, CephMetricStats{
			"read",
			pool.Stats.ReadBytes.String(),
			"bytes",
			const_label,
			"total",
			var_label, "Total read throughput for the pool"})

		returnMap = append(returnMap, CephMetricStats{
			"write",
			pool.Stats.WriteIO.String(),
			"", const_label,
			"total",
			var_label, "Total write i/o calls for the pool"})

		returnMap = append(returnMap, CephMetricStats{
			"write",
			pool.Stats.WriteBytes.String(),
			"bytes",
			const_label,
			"total",
			var_label, "Total write throughput for the pool"})
	}
	return returnMap, nil
}

func (cli *MetricCli) CollectClusterMetrics() ([]CephMetricStats, error) {
	var returnMap []CephMetricStats

	returnMap = []CephMetricStats{}
	const_label := make(map[string]string)
	const_label["cluster"] = "ceph"
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "df",
		"detail": "detail",
		"format": "json",
	})
	if err != nil {
		log.Errorf("cmd failed with %s\n", err)
	}

	cmd, err = json.Marshal(map[string]interface{}{
		"prefix": "df",
		"detail": "detail",
		"format": "json",
	})
	if err != nil {
		// panic! because ideally in no world this hard-coded input
		// should fail.
		panic(err)
	}
	buf, _, err := cli.conn.MonCommand(cmd)
	if err != nil {
	}
	cluster_stats := &cephClusterStats{}
	if err := json.Unmarshal(buf, cluster_stats); err != nil {

		log.Fatalf("Unmarshal error: %v", err)
		// return
	}

	returnMap = append(returnMap,
		CephMetricStats{
			"capacity",
			cluster_stats.Stats.TotalBytes.String(),
			"bytes",
			const_label,
			"",
			nil, ""},
		CephMetricStats{
			"available",
			cluster_stats.Stats.TotalAvailBytes.String(),
			"bytes",
			const_label,
			"",
			nil, ""},
		CephMetricStats{
			"used",
			cluster_stats.Stats.TotalUsedBytes.String(),
			"bytes",
			const_label,
			"",
			nil, ""},
		CephMetricStats{
			"objects",
			cluster_stats.Stats.TotalObjects.String(),
			"",
			const_label,
			"", nil, ""},
	)
	return returnMap, nil
}

func (cli *MetricCli) CollectPerfMetrics() ([]CephMetricStats, error) {
	var returnMap []CephMetricStats
	returnMap = []CephMetricStats{}
	const_label := make(map[string]string)
	const_label["cluster"] = "ceph"
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
		var_label := make(map[string]string)

		osdID, err := perfStat.ID.Int64()
		if err != nil {
			log.Errorf("when collecting ceph cluster metrics")
		}
		var_label["osd"] = fmt.Sprintf("osd.%v", osdID)

		returnMap = append(returnMap,
			CephMetricStats{
				"perf_commit_latency",
				perfStat.Stats.CommitLatency.String(),
				"ms",
				const_label,
				"",
				var_label, ""},
			CephMetricStats{
				"perf_apply_latency",
				perfStat.Stats.ApplyLatency.String(),
				"ms",
				const_label,
				"",
				var_label, ""})

	}
	return returnMap, nil
}

func (cli *MetricCli) CollectOsddfMetrics() ([]CephMetricStats, error) {
	var returnMap []CephMetricStats
	returnMap = []CephMetricStats{}
	const_label := make(map[string]string)
	const_label["cluster"] = "ceph"
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd df",
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	buf, _, err := cli.conn.MonCommand(cmd)
	if err != nil {
		log.Errorf("unable to collect data from ceph osd df")
	}
	osddf := &cephOSDDF{}
	if err := json.Unmarshal(buf, osddf); err != nil {
		log.Errorf("unmarshal failed")
	}
	for _, osd_df := range osddf.OSDNodes {
		var_label := make(map[string]string)
		var_label["osd"] = osd_df.Name.String()
		returnMap = append(returnMap,
			CephMetricStats{
				"osd_crush_weight",
				osd_df.CrushWeight.String(),
				"bytes", const_label,
				"",
				var_label, ""})

	}
	returnMap = append(returnMap, CephMetricStats{
		"osd_total_bytes",
		osddf.Summary.TotalKB.String(),
		"bytes",
		const_label,
		"",
		nil, ""},
		CephMetricStats{
			"osd_total_used_bytes",
			osddf.Summary.TotalUsedKB.String(),
			"bytes",
			const_label,
			"",
			nil, ""},
		CephMetricStats{
			"total_avail",
			osddf.Summary.TotalAvailKB.String(),
			"bytes",
			const_label,
			"",
			nil, ""},
		CephMetricStats{
			"osd_average_utilization",
			osddf.Summary.AverageUtil.String(),
			"",
			const_label,
			"",
			nil, ""})

	return returnMap, nil
}

func (cli *MetricCli) CollectOsddumpMetrics() ([]CephMetricStats, error) {
	var returnMap []CephMetricStats
	returnMap = []CephMetricStats{}
	const_label := make(map[string]string)
	const_label["cluster"] = "ceph"
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd dump",
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	buf, _, err := cli.conn.MonCommand(cmd)
	if err != nil {
		log.Errorf("unable to collect data from ceph osd perf")
	}
	osd_dump := &cephOSDDump{}
	if err := json.Unmarshal(buf, osd_dump); err != nil {
		log.Errorf("unmarshal failed")
	}
	var_label := make(map[string]string)
	var_label["osd"] = fmt.Sprintf("osd.%s", osd_dump.OSDs[0].OSD.String())
	returnMap = append(returnMap,
		CephMetricStats{
			"osd_up",
			osd_dump.OSDs[0].Up.String(),
			"",
			const_label,
			"",
			var_label, ""},
		CephMetricStats{
			"osd_in",
			osd_dump.OSDs[0].In.String(),
			"",
			const_label,
			"",
			var_label, ""})
	return returnMap, nil
}

func (cli *MetricCli) CollectHealthMetrics() ([]CephMetricStats, error) {
	returnMap := []CephMetricStats{}
	constlabel := make(map[string]string)
	constlabel["cluster"] = "ceph"
	health_cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "status",
		"format": "json",
	})
	if err != nil {
		log.Errorf("cmd failed with %s\n", err)
	}
	buff, _, err := cli.conn.MonCommand(health_cmd)
	if err != nil {
	}
	health_stats := &cephHealthStats{}
	if err := json.Unmarshal(buff, health_stats); err != nil {
		log.Fatalf("Unmarshal error: %v", err)
	}

	returnMap = append(returnMap, CephMetricStats{
		"client_io_write",
		health_stats.PGMap.WriteOpPerSec.String(),
		"", constlabel,
		"ops",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"client_io_read",
		health_stats.PGMap.ReadOpPerSec.String(),
		"", constlabel,
		"ops",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"io",
		(health_stats.PGMap.ReadOpPerSec.String() + health_stats.PGMap.WriteOpPerSec.String()),
		"",
		constlabel,
		"ops",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"client_io_write",
		health_stats.PGMap.WriteBytePerSec.String(),
		"bytes",
		constlabel,
		"",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"cache_evict_io",
		health_stats.PGMap.CacheEvictBytePerSec.String(),
		"bytes",
		constlabel,
		"",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"cache_promote_io",
		health_stats.PGMap.CachePromoteOpPerSec.String(),
		"",
		constlabel,
		"ops",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"DegradedObjects",
		health_stats.PGMap.DegradedObjects.String(),
		"", constlabel,
		"",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"MisplacedObjects",
		health_stats.PGMap.MisplacedObjects.String(),
		"",
		constlabel,
		"",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"osds",
		health_stats.OSDMap.OSDMap.NumOSDs.String(),
		"",
		constlabel,
		"",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"osds",
		health_stats.OSDMap.OSDMap.NumUpOSDs.String(),
		"",
		constlabel,
		"up",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"osds",
		health_stats.OSDMap.OSDMap.NumInOSDs.String(),
		"",
		constlabel,
		"in",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"pgs_remapped",
		health_stats.OSDMap.OSDMap.NumRemappedPGs.String(),
		"", constlabel,
		"",
		nil, ""})

	returnMap = append(returnMap, CephMetricStats{
		"total_pgs",
		health_stats.PGMap.NumPGs.String(),
		"",
		constlabel,
		"",
		nil, ""})
	return returnMap, nil
}

func (cli *MetricCli) CollectMetrics(metricList []string, instanceID string) ([]CephMetricStats, error) {
	returnMap := []CephMetricStats{}

	//Collecting Pool Metrics
	pool_metric, _ := cli.CollectPoolMetrics()
	for i := range pool_metric {
		returnMap = append(returnMap, pool_metric[i])
	}
	cluster_metric, _ := cli.CollectClusterMetrics()
	for i := range cluster_metric {
		returnMap = append(returnMap, cluster_metric[i])
	}

	perf_metric, _ := cli.CollectPerfMetrics()
	for i := range perf_metric {
		returnMap = append(returnMap, perf_metric[i])
	}

	osd_df_metric, _ := cli.CollectOsddfMetrics()
	for i := range osd_df_metric {
		returnMap = append(returnMap, osd_df_metric[i])
	}

	osd_dump_metric, _ := cli.CollectOsddumpMetrics()
	for i := range osd_dump_metric {
		returnMap = append(returnMap, osd_dump_metric[i])
	}

	health_metrics, _ := cli.CollectHealthMetrics()
	for i := range health_metrics {
		returnMap = append(returnMap, health_metrics[i])
	}

	// TODO Collecting Ceph Health Metrics

	// TODO Collecting Monitors Metrics

	return returnMap, nil
}
