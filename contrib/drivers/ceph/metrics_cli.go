package ceph

import (
	"encoding/json"
	"fmt"
	"github.com/ceph/go-ceph/rados"
)

type MetricCli struct {
}
type cephClusterStats struct {
	Stats struct {
		TotalBytes      json.Number `json:"total_bytes"`
		TotalUsedBytes  json.Number `json:"total_used_bytes"`
		TotalAvailBytes json.Number `json:"total_avail_bytes"`
		TotalObjects    json.Number `json:"total_objects"`
	} `json:"stats"`
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
type cephHealthDetailStats struct {
	Checks map[string]struct {
		Details []struct {
			Message string `json:"message"`
		} `json:"detail"`
		Summary struct {
			Message string `json:"message"`
		} `json:"summary"`
		Severity string `json:"severity"`
	} `json:"checks"`
}

//	CollectMetrics function is call the cli for metrics collection. This will be invoked  by ceph metric driver
//	metricList	-> metrics to be collected
//	instanceID	-> for which instance to be collected
//	returnMap	-> metrics to value map
func CollectMetrics(metricList []string, instanceID string, resource string) (map[string]string, error) {

	returnMap := make(map[string]string)
	var err error
	conn, err := rados.NewConn()
	if err != nil {

		fmt.Println(err)

	}

	err = conn.ReadDefaultConfigFile()
	err = conn.Connect()
	if err != nil {

		fmt.Println(err)
	}
	switch resource {

	case "health":

		cmd, err := json.Marshal(map[string]interface{}{
			"prefix": "status",
			"format": "json",
		})
		if err != nil {
			panic(err)
		}
		buf, _, err := conn.MonCommand(cmd)
		if err != nil {
			panic(err)
		}
		stats := &cephHealthStats{}
		if err := json.Unmarshal(buf, stats); err != nil {
			fmt.Printf("error")

		}
		//fmt.Printf("Command Output: %v",stats)

		//cephHealthStats ->OSDMap ->	OSDMap

		returnMap["NumOSDs"] = stats.OSDMap.OSDMap.NumOSDs.String()
		returnMap["NumUpOSDs"] = stats.OSDMap.OSDMap.NumUpOSDs.String()
		returnMap["NumInOSDs"] = stats.OSDMap.OSDMap.NumInOSDs.String()
		returnMap["NumRemappedPGs"] = stats.OSDMap.OSDMap.NumRemappedPGs.String()

		//cephHealthStats -> OSDMap ->PGMap

		returnMap["NumPGs"] = stats.PGMap.NumPGs.String()
		returnMap["WriteOpPerSec"] = stats.PGMap.WriteOpPerSec.String()
		returnMap["ReadOpPerSec"] = stats.PGMap.ReadOpPerSec.String()
		returnMap["WriteBytePerSec"] = stats.PGMap.WriteBytePerSec.String()
		returnMap["ReadBytePerSec"] = stats.PGMap.ReadBytePerSec.String()
		returnMap["RecoveringObjectsPerSec"] = stats.PGMap.RecoveringObjectsPerSec.String()
		returnMap["RecoveringBytePerSec"] = stats.PGMap.RecoveringBytePerSec.String()
		returnMap["RecoveringKeysPerSec"] = stats.PGMap.RecoveringKeysPerSec.String()
		returnMap["CacheFlushBytePerSec"] = stats.PGMap.CacheFlushBytePerSec.String()
		returnMap["CacheEvictBytePerSec"] = stats.PGMap.CacheEvictBytePerSec.String()
		returnMap["CachePromoteOpPerSec"] = stats.PGMap.CachePromoteOpPerSec.String()
		returnMap["DegradedObjects"] = stats.PGMap.DegradedObjects.String()
		returnMap["MisplacedObjects"] = stats.PGMap.MisplacedObjects.String()

	case "cluster":
		cmd, err := json.Marshal(map[string]interface{}{
			"prefix": "df",
			"detail": "detail",
			"format": "json",
		})
		if err != nil {
			// panic! because ideally in no world this hard-coded input
			// should fail.
			panic(err)
		}
		buf, _, err := conn.MonCommand(cmd)
		if err != nil {
		}
		//st := &cephClusterStats{}
		//if err := json.Unmarshal(buf, st); err != nil {
		//	fmt.Printf("error")
		//	//return
		//}
		st := &cephClusterStats{}
		if err := json.Unmarshal(buf, st); err != nil {
			fmt.Printf("error")
			//return
		}

		fmt.Printf("Command Output: %v", st)

		returnMap["TotalBytes"] = st.Stats.TotalBytes.String()
	//returnMap["TotalUsedBytes"] =st.Stats.TotalUsedBytes.String()
	//returnMap["TotalAvailBytes"] =st.Stats.TotalAvailBytes.String()
	default:
		fmt.Printf("Invalid recource Name %v \n", resource)

	}
	conn.Shutdown()

	return returnMap, err
}
