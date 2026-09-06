package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cube-js/cube-operator/internal/leadership"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const DefaultPromotionFile = "/var/run/cubestore-promotion/promotion.json"
const promotionDataKey = "route-role.json"
const maxPromotionBytes = 1 << 20

var ErrInvalidPromotion = errors.New("promotion marker does not match the authoritative lease")

// PromotionSource is read-only. The agent never elects or promotes a Router;
// it only mirrors a controller-published marker for the exact current lease.
type PromotionSource interface {
	Read(context.Context) ([]byte, error)
}

type ConfigMapPromotionSource struct {
	Reader    client.Reader
	Namespace string
	Name      string
}

func (s *ConfigMapPromotionSource) Read(ctx context.Context) ([]byte, error) {
	var cm corev1.ConfigMap
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, &cm); err != nil {
		return nil, err
	}
	raw, ok := cm.Data[promotionDataKey]
	if !ok {
		return nil, fmt.Errorf("promotion ConfigMap %s/%s is missing %s", s.Namespace, s.Name, promotionDataKey)
	}
	return []byte(raw), nil
}

type promotionDocument struct {
	ActiveLeader   string `json:"activeLeader"`
	LeaderEpoch    int64  `json:"leaderEpoch"`
	LeaseClusterID string `json:"leaseClusterID"`
	LeaseEpoch     int64  `json:"leaseEpoch"`
	LeaseToken     string `json:"leaseToken"`
}

func validatePromotion(raw []byte, lease leadership.LeaseRecord) error {
	if len(raw) == 0 || len(raw) > maxPromotionBytes {
		return ErrInvalidPromotion
	}
	var marker promotionDocument
	if err := json.Unmarshal(raw, &marker); err != nil {
		return ErrInvalidPromotion
	}
	if marker.ActiveLeader != lease.HolderID || marker.LeaderEpoch != lease.Epoch || marker.LeaseClusterID != lease.ClusterID || marker.LeaseEpoch != lease.Epoch || marker.LeaseToken != lease.Token {
		return ErrInvalidPromotion
	}
	return nil
}
func (a *Agent) fenceLocal() error {
	leaseErr := a.writeExpiredFollower()
	var promotionErr error
	if a.promotionSource != nil {
		// Empty activeLeader/zero epochs cannot acknowledge any promotion. Keep
		// the existing JSON field contract while revoking the local marker.
		promotionErr = writeAtomic(a.promotionPath, promotionDocument{})
	}
	return errors.Join(leaseErr, promotionErr)
}
