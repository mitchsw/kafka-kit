package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	restfake "k8s.io/client-go/rest/fake"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// Three valid broker pods distributed over two nodes, and some invalid pods that will be ignored.
func testObjects() []runtime.Object {
	return []runtime.Object{
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "broker-1",
				Namespace: "test-ns",
				Labels:    map[string]string{"cluster": "foo", "kafka_broker_id": "101"},
			},
			Spec: v1.PodSpec{
				NodeName: "node-a",
				Volumes: []v1.Volume{
					{
						Name: "broker-1-local",
						VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "broker-1-local-node-a-claim",
						}},
					},
					{Name: "another-vol"},
				},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "broker-2",
				Namespace: "test-ns",
				Labels:    map[string]string{"cluster": "foo", "kafka_broker_id": "102"},
			},
			Spec: v1.PodSpec{
				NodeName: "node-b",
				Volumes: []v1.Volume{
					{
						Name: "broker-2-local",
						VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "broker-2-local-node-b-claim",
						}},
					},
				},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "broker-3",
				Namespace: "test-ns",
				Labels:    map[string]string{"cluster": "foo", "kafka_broker_id": "103"},
			},
			Spec: v1.PodSpec{
				NodeName: "node-a",
				Volumes: []v1.Volume{
					{
						Name: "broker-3-local",
						VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "broker-3-local-node-a-claim",
						}},
					},
				},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-volume",
				Namespace: "test-ns",
				Labels:    map[string]string{"cluster": "foo", "kafka_broker_id": "103"},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-broker-id",
				Namespace: "test-ns",
				Labels:    map[string]string{"cluster": "foo"},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bad-broker-id",
				Namespace: "test-ns",
				Labels:    map[string]string{"cluster": "foo", "kafka_broker_id": "not-int"},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "different-cluster",
				Namespace: "test-ns",
				Labels:    map[string]string{"cluster": "bar"},
			},
		},
		&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "different-labels",
				Namespace: "test-ns",
				Labels:    map[string]string{}, // No cluster
			},
		}}
}

func uint64Ptr(value float64) *uint64 {
	val := uint64(value)
	return &val
}

// Fragment of the stats returned for node-a.
func testNodeAStats() statsapi.Summary {
	return statsapi.Summary{
		Pods: []statsapi.PodStats{
			{
				PodRef: statsapi.PodReference{Namespace: "test-ns", Name: "broker-1"},
				VolumeStats: []statsapi.VolumeStats{
					{
						Name: "broker-1-local",
						PVCRef: &statsapi.PVCReference{
							Namespace: "test-ns",
							Name:      "broker-1-local-node-a-claim",
						},
						FsStats: statsapi.FsStats{
							CapacityBytes:  uint64Ptr(1000),
							AvailableBytes: uint64Ptr(600),
							UsedBytes:      uint64Ptr(400),
						},
					},
					{
						Name: "another-vol",
						FsStats: statsapi.FsStats{
							CapacityBytes:  uint64Ptr(100),
							AvailableBytes: uint64Ptr(90),
							UsedBytes:      uint64Ptr(10),
						},
					},
				},
			},
			{
				PodRef: statsapi.PodReference{Namespace: "test-ns", Name: "broker-3"},
				VolumeStats: []statsapi.VolumeStats{
					{
						Name: "broker-3-local",
						PVCRef: &statsapi.PVCReference{
							Namespace: "test-ns",
							Name:      "broker-3-local-node-a-claim",
						},
						FsStats: statsapi.FsStats{
							CapacityBytes:  uint64Ptr(200000),
							AvailableBytes: uint64Ptr(160000),
							UsedBytes:      uint64Ptr(40000),
						},
					},
				},
			},
		},
	}
}

// Fragment of the stats returned for node-b.
func testNodeBStats() statsapi.Summary {
	return statsapi.Summary{
		Pods: []statsapi.PodStats{
			{
				PodRef: statsapi.PodReference{Namespace: "test-ns", Name: "broker-2"},
				VolumeStats: []statsapi.VolumeStats{
					{
						Name: "broker-2-local",
						PVCRef: &statsapi.PVCReference{
							Namespace: "test-ns",
							Name:      "broker-2-local-node-b-claim",
						},
						FsStats: statsapi.FsStats{
							CapacityBytes:  uint64Ptr(5000),
							AvailableBytes: uint64Ptr(2000),
							UsedBytes:      uint64Ptr(3000),
						},
					},
				},
			},
		},
	}
}

func objBody(object interface{}) io.ReadCloser {
	output, err := json.Marshal(object)
	if err != nil {
		panic(err)
	}
	return ioutil.NopCloser(bytes.NewReader([]byte(output)))
}

func TestVolumeStatsReader(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset(testObjects()...)
	httpClient := restfake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		header := http.Header{}
		header.Set("Content-Type", runtime.ContentTypeJSON)
		resp := &http.Response{StatusCode: 200, Header: header}
		if strings.Contains(req.URL.Path, "/nodes/node-a/proxy/stats/summary") {
			resp.Body = objBody(testNodeAStats())
		} else if strings.Contains(req.URL.Path, "/nodes/node-b/proxy/stats/summary") {
			resp.Body = objBody(testNodeBStats())
		} else {
			return nil, fmt.Errorf("Unexpected restClient path %v", req.URL.Path)
		}
		return resp, nil
	})
	restClient := &restfake.RESTClient{}
	restClient.Client = httpClient
	vsr := VolumeStatsReader{kubeClient, restClient}

	// Expect the correct volume stats for the three brokers.
	res, err := vsr.Get("test-ns", "cluster=foo")
	assert.Nil(t, err, "error is not nil")
	expectedRes := []VolumeStats{
		{
			Pod:                   "broker-1",
			Node:                  "node-a",
			BrokerId:              101,
			PersistentVolumeClaim: "broker-1-local-node-a-claim",
			AvailableBytes:        600,
			CapacityBytes:         1000,
			UsedBytes:             400,
		},
		{
			Pod:                   "broker-2",
			Node:                  "node-b",
			BrokerId:              102,
			PersistentVolumeClaim: "broker-2-local-node-b-claim",
			AvailableBytes:        2000,
			CapacityBytes:         5000,
			UsedBytes:             3000},
		{
			Pod:                   "broker-3",
			Node:                  "node-a",
			BrokerId:              103,
			PersistentVolumeClaim: "broker-3-local-node-a-claim",
			AvailableBytes:        160000,
			CapacityBytes:         200000,
			UsedBytes:             40000,
		},
	}
	assert.ElementsMatch(t, res, expectedRes, "results do not match")

	// Test matcher arguments.
	res, err = vsr.Get("unknown-ns", "cluster=foo")
	assert.Nil(t, err, "error is not nil")
	assert.Empty(t, res, "expected no response")

	res, err = vsr.Get("test-ns", "cluster=unknown_cluster")
	assert.Nil(t, err, "error is not nil")
	assert.Empty(t, res, "expected no response")
}
