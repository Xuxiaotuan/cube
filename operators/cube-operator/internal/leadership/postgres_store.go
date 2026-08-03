package leadership

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var postgresTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

type PostgresStore struct {
	pool  *pgxpool.Pool
	table string
}

func NewPostgresStore(pool *pgxpool.Pool, table string) (*PostgresStore, error) {
	if pool == nil {
		return nil, fmt.Errorf("PostgreSQL pool is required")
	}
	table = strings.TrimSpace(table)
	if table == "" {
		table = "cubestore_router_leases"
	}
	if !postgresTableName.MatchString(table) {
		return nil, fmt.Errorf("invalid PostgreSQL lease table %q", table)
	}
	return &PostgresStore{pool: pool, table: table}, nil
}

func (s *PostgresStore) Ensure(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		cluster_id text PRIMARY KEY,
		holder_id text NOT NULL,
		token text NOT NULL,
		epoch bigint NOT NULL,
		issued_at timestamptz NOT NULL,
		expires_at timestamptz NOT NULL
	)`, s.table))
	return err
}

func (s *PostgresStore) Acquire(ctx context.Context, clusterID, holderID string, ttl time.Duration) (LeaseRecord, bool, error) {
	if err := validateLeaseRequest(clusterID, holderID, ttl); err != nil {
		return LeaseRecord{}, false, err
	}
	token, err := newLeaseToken()
	if err != nil {
		return LeaseRecord{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (cluster_id, holder_id, token, epoch, issued_at, expires_at)
		VALUES ($1, '', '', 0, clock_timestamp(), clock_timestamp()) ON CONFLICT (cluster_id) DO NOTHING`, s.table), clusterID); err != nil {
		return LeaseRecord{}, false, err
	}
	current, active, err := s.lockedLease(ctx, tx, clusterID)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	if active {
		if err := tx.Commit(ctx); err != nil {
			return LeaseRecord{}, false, err
		}
		return current, false, nil
	}
	record, err := s.writeLease(ctx, tx, clusterID, holderID, token, ttl, true)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseRecord{}, false, err
	}
	return record, true, nil
}

func (s *PostgresStore) Renew(ctx context.Context, lease LeaseRecord, ttl time.Duration) (LeaseRecord, bool, error) {
	if err := validateLeaseRequest(lease.ClusterID, lease.HolderID, ttl); err != nil {
		return LeaseRecord{}, false, err
	}
	if err := validateLeaseIdentity(lease); err != nil {
		return LeaseRecord{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, active, err := s.lockedLease(ctx, tx, lease.ClusterID)
	if errors.Is(err, pgx.ErrNoRows) {
		return LeaseRecord{}, false, ErrLeaseNotFound
	}
	if err != nil {
		return LeaseRecord{}, false, err
	}
	if !active || ValidateLeaseFence(current, lease) != nil {
		if err := tx.Commit(ctx); err != nil {
			return LeaseRecord{}, false, err
		}
		return current, false, nil
	}
	record, err := s.writeLease(ctx, tx, lease.ClusterID, lease.HolderID, lease.Token, ttl, false)
	if err != nil {
		return LeaseRecord{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseRecord{}, false, err
	}
	return record, true, nil
}

func (s *PostgresStore) Release(ctx context.Context, lease LeaseRecord) error {
	if err := validateLeaseIdentity(lease); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET holder_id = '', token = '', expires_at = clock_timestamp()
		WHERE cluster_id = $1 AND holder_id = $2 AND token = $3 AND epoch = $4 AND expires_at > clock_timestamp()`, s.table), lease.ClusterID, lease.HolderID, lease.Token, lease.Epoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleLease
	}
	return nil
}

func (s *PostgresStore) Get(ctx context.Context, clusterID string) (LeaseRecord, error) {
	if strings.TrimSpace(clusterID) == "" {
		return LeaseRecord{}, fmt.Errorf("cluster ID is required")
	}
	record, active, err := s.queryLease(ctx, s.pool, clusterID, false)
	if errors.Is(err, pgx.ErrNoRows) || !active {
		return LeaseRecord{}, ErrLeaseNotFound
	}
	return record, err
}

type postgresQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *PostgresStore) lockedLease(ctx context.Context, tx pgx.Tx, clusterID string) (LeaseRecord, bool, error) {
	return s.queryLease(ctx, tx, clusterID, true)
}

func (s *PostgresStore) queryLease(ctx context.Context, queryer postgresQueryer, clusterID string, lock bool) (LeaseRecord, bool, error) {
	query := fmt.Sprintf(`SELECT cluster_id, holder_id, token, epoch, issued_at, expires_at, expires_at > clock_timestamp() FROM %s WHERE cluster_id = $1`, s.table)
	if lock {
		query += " FOR UPDATE"
	}
	var record LeaseRecord
	var active bool
	err := queryer.QueryRow(ctx, query, clusterID).Scan(&record.ClusterID, &record.HolderID, &record.Token, &record.Epoch, &record.IssuedAt, &record.ExpiresAt, &active)
	return record, active, err
}

func (s *PostgresStore) writeLease(ctx context.Context, tx pgx.Tx, clusterID, holderID, token string, ttl time.Duration, incrementEpoch bool) (LeaseRecord, error) {
	epoch := "epoch"
	if incrementEpoch {
		epoch = "epoch + 1"
	}
	query := fmt.Sprintf(`UPDATE %s SET holder_id = $2, token = $3, epoch = %s, issued_at = CASE WHEN $5 THEN clock_timestamp() ELSE issued_at END,
		expires_at = clock_timestamp() + $4::interval WHERE cluster_id = $1
		RETURNING cluster_id, holder_id, token, epoch, issued_at, expires_at`, s.table, epoch)
	var record LeaseRecord
	err := tx.QueryRow(ctx, query, clusterID, holderID, token, postgresInterval(ttl), incrementEpoch).Scan(
		&record.ClusterID, &record.HolderID, &record.Token, &record.Epoch, &record.IssuedAt, &record.ExpiresAt,
	)
	return record, err
}

func postgresInterval(ttl time.Duration) string {
	return fmt.Sprintf("%d microseconds", ttl.Microseconds())
}
