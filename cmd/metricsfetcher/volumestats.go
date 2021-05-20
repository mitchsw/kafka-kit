package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// VolumeStatsReader fetches volume statistics about Kafka brokers from Kubernetes APIs.
// It cannot query this directly from broker pods, and instead queries their node's
// kubelet /stats.
type VolumeStatsReader struct {
	kubeClient kubernetes.Interface
	restClient restclient.Interface
}

func NewVolumeStatsReader(kubeClient kubernetes.Interface) VolumeStatsReader {
	return VolumeStatsReader{kubeClient, kubeClient.CoreV1().RESTClient()}
}

type VolumeStats struct {
	Pod  string
	Node string

	// BrokerId extracted from Pod's kafka_broker_id label.
	BrokerId int

	PersistentVolumeClaim string
	AvailableBytes        uint64
	CapacityBytes         uint64
	UsedBytes             uint64
}

func (r *VolumeStatsReader) Get(namespace, podLabelSelector string) ([]VolumeStats, error) {
	pods, err := r.kubeClient.CoreV1().
		Pods(namespace).List(context.Background(), metav1.ListOptions{LabelSelector: podLabelSelector})
	if err != nil {
		return nil, err
	}
	var results []VolumeStats
	for _, p := range pods.Items {
		brokerId, err := getBrokerId(&p)
		if err != nil {
			fmt.Printf("skipping pod %v: %v\n", p.Name, err)
			continue
		}
		pvcName, err := getPVCName(&p)
		if err != nil {
			fmt.Printf("skipping pod %v: %v\n", p.Name, err)
			continue
		}

		// TODO: for large clusters, consider async producers to pipeline many API calls.
		vs, err := r.getVolumeStats(p.Spec.NodeName, namespace, pvcName)
		if err != nil {
			fmt.Printf("skipping pod %v: querying volume stats, %v\n", p.Name, err)
			continue
		}

		results = append(results, VolumeStats{
			BrokerId:              brokerId,
			Pod:                   p.Name,
			Node:                  p.Spec.NodeName,
			PersistentVolumeClaim: pvcName,
			AvailableBytes:        *vs.AvailableBytes,
			CapacityBytes:         *vs.CapacityBytes,
			UsedBytes:             *vs.UsedBytes,
		})
	}
	return results, nil
}

func getBrokerId(p *v1.Pod) (int, error) {
	brokerIdStr, ok := p.Labels["kafka_broker_id"]
	if !ok {
		return 0, errors.New("no kafka_broker_id")
	}
	var brokerId int
	var err error
	if brokerId, err = strconv.Atoi(brokerIdStr); err != nil {
		return 0, err
	}
	return brokerId, nil
}

func getPVCName(p *v1.Pod) (string, error) {
	for _, v := range p.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			// Ignore volumes that are not backed by a PersistentVolume.
			continue
		}
		return v.PersistentVolumeClaim.ClaimName, nil
	}
	return "", errors.New("cannot find a PersistentVolumeClaim")
}

// There are no Pods API to get volume stats. Instead, we must to query the Node and filter to
// the PVC of interest. This requires `GET nodes/proxy` permission to access the kubelet's /stats API.
func (r *VolumeStatsReader) getVolumeStats(nodeName, namespace, pvc string) (*statsapi.VolumeStats, error) {
	request := r.restClient.Get().Resource("nodes").Name(nodeName).SubResource("proxy").Suffix("stats/summary")
	rawResp, err := request.DoRaw(context.Background())
	if err != nil {
		return nil, err
	}
	var stats statsapi.Summary
	if err := json.Unmarshal(rawResp, &stats); err != nil {
		return nil, err
	}
	for _, p := range stats.Pods {
		for _, v := range p.VolumeStats {
			if v.PVCRef != nil && v.PVCRef.Namespace == namespace && v.PVCRef.Name == pvc {
				return &v, nil
			}
		}
	}
	return nil, fmt.Errorf("could not find PersistentVolumeClaim %v", pvc)
}
