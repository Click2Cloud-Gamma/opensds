package ceph

import (
	"fmt"

	log "github.com/golang/glog"
	"github.com/opensds/opensds/pkg/model"
	"gopkg.in/yaml.v2"
	"strconv"
	"time"
)

// Todo: Move this Yaml config to a file

var data = `
resources:
 - resource: health
   metrics:
    - Severity
    - Summary
    - Message
    - OverallStatus
    - Status
    - NumOSDs
    - NumUpOSDs
    - NumInOSDs
    - NumRemappedPGs
    - NumPGs
    - WriteOpPerSec
    - ReadOpPerSec           
    - WriteBytePerSec
    - ReadBytePerSec
    - RecoveringObjectsPerSec
    - RecoveringBytePerSec
    - RecoveringKeysPerSec
    - CacheFlushBytePerSec
    - CacheEvictBytePerSec
    - CachePromoteOpPerSec
    - DegradedObjects
    - MisplacedObjects
    - Count
    - States
   units:
    - ""
    - ""
    - ""
    - ""
    - ""
    - ""
    - ""
    - ""
    - ""
    - ""
    - Op/s
    - Op/s
    - bytes/s
    - bytes/s
    - objects/s
    - bytes/s
    - keys/s
    - bytes/s
    - bytes/s
    - Op/s
    - ""
    - ""
    - ""
    - ""
 - resource: cluster
   metrics:
    - TotalBytes
   units:
    - B
`

type Config struct {
	Resource string
	Metrics  []string
	Units    []string
}

type Configs struct {
	Cfgs []Config `resources`
}
type MetricDriver struct {
}

func metricInMetrics(metric string, metriclist []string) bool {
	for _, m := range metriclist {
		if m == metric {
			return true
		}
	}
	return false
}
func getCurrentUnixTimestamp() int64 {
	now := time.Now()
	secs := now.Unix()
	return secs
}
func getMetricToUnitMap() map[string]string {

	//construct metrics to value map
	var configs Configs
	//Read supported metric list from yaml config
	//Todo: Move this to read from file
	source := []byte(data)

	error := yaml.Unmarshal(source, &configs)
	if error != nil {
		log.Fatalf("Unmarshal error: %v", error)
	}
	metricToUnitMap := make(map[string]string)
	for _, resources := range configs.Cfgs {
		switch resources.Resource {
		//ToDo: Other Cases needs to be added
		case "health":
			for index, metricName := range resources.Metrics {

				metricToUnitMap[metricName] = resources.Units[index]

			}
		case "cluster":
			for index, metricName := range resources.Metrics {

				metricToUnitMap[metricName] = resources.Units[index]

			}
		}
	}
	return metricToUnitMap
}

// 	ValidateMetricsSupportList:- is  to check whether the posted metric list is in the uspport list of this driver
// 	metricList-> Posted metric list
//	supportedMetrics -> list of supported metrics
func (d *MetricDriver) ValidateMetricsSupportList(metricList []string, resourceType string) (supportedMetrics []string, err error) {
	var configs Configs

	//Read supported metric list from yaml config
	//Todo: Move this to read from file
	source := []byte(data)
	error := yaml.Unmarshal(source, &configs)
	if error != nil {
		log.Fatalf("Unmarshal error: %v", error)
	}

	for _, resources := range configs.Cfgs {

		if resources.Resource == resourceType {
			for _, metricName := range metricList {
				if metricInMetrics(metricName, resources.Metrics) {
					supportedMetrics = append(supportedMetrics, metricName)

				} else {
					fmt.Printf("\n metric: %v is not in the supported list", metricName)
				}
			}

		}
		//switch resources.Resource {
		////ToDo: Other Cases needs to be added
		//case resourceType:
		//	for _, metricName := range metricList {
		//		if metricInMetrics(metricName, resources.Metrics) {
		//			supportedMetrics = append(supportedMetrics, metricName)
		//
		//
		//		} else {
		//			fmt.Printf("\n metric: %v is not in the supported list", metricName)
		//		}
		//	}
		//default:
		//	break
		//
		//
		//}

	}

	return supportedMetrics, nil
}

//	CollectMetrics: Driver entry point to collect metrics. This will be invoked by the dock
//	metricsList-> posted metric list
//	instanceID -> posted instanceID
//	metricArray	-> the array of metrics to be returned
func (d *MetricDriver) CollectMetrics(metricsList []string, instanceID string, resource string) ([]*model.MetricSpec, error) {

	// get Metrics to unit map
	metricToUnitMap := getMetricToUnitMap()
	//validate metric support list
	supportedMetrics, err := d.ValidateMetricsSupportList(metricsList, resource)
	if supportedMetrics == nil {
		log.Infof("No metrics found in the  supported metric list")
	}
	metricMap, err := CollectMetrics(supportedMetrics, instanceID, resource)

	var tempMetricArray []*model.MetricSpec
	for _, element := range supportedMetrics {

		val, _ := strconv.ParseFloat(metricMap[element], 64)
		//Todo: See if association  is required here, resource discovery could fill this information
		associatorMap := make(map[string]string)
		associatorMap["Cluster"] = "ceph"
		metricValue := &model.Metric{
			Timestamp: getCurrentUnixTimestamp(),
			Value:     val,
		}
		metricValues := make([]*model.Metric, 0)
		metricValues = append(metricValues, metricValue)

		metric := &model.MetricSpec{
			InstanceID:   instanceID,
			InstanceName: metricMap["InstanceName"],
			Job:          "OpenSDS",
			Labels:       associatorMap,
			//Todo Take Componet from Post call, as of now it is only for volume
			Component: "Volume",
			Name:      element,
			//Todo : Fill units according to metric type
			Unit: metricToUnitMap[element],
			//Todo : Get this information dynamically ( hard coded now , as all are direct values
			AggrType:     "",
			MetricValues: metricValues,
		}
		tempMetricArray = append(tempMetricArray, metric)
	}
	metricArray := tempMetricArray
	return metricArray, err
}

func (d *MetricDriver) Setup() error {

	return nil
}

func (*MetricDriver) Teardown() error { return nil }
