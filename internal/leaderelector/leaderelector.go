// Package leaderelector wraps client-go's tools/leaderelection so the rest
// of the agent only sees a simple "am I the leader?" flag. It uses a
// coordination.k8s.io/v1 Lease object as the lock, the same primitive
// kube-controller-manager and Argo CD rely on.
package leaderelector

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Options drives the lease behaviour. The defaults mirror what
// kube-controller-manager ships with — well-known, safe values.
type Options struct {
	// LeaseName is the Lease resource name. Must be unique per deployment
	// of the agent in a given namespace.
	LeaseName string

	// Namespace where the Lease lives. Same namespace as the pods so the
	// ClusterRoleBinding can stay scoped to a Role.
	Namespace string

	// Identity uniquely identifies this pod. Set to POD_NAME (downward API)
	// in production so kubectl describe lease shows a meaningful holder.
	Identity string

	// LeaseDuration is how long a non-leader will wait before forcibly
	// acquiring an expired lease. 15s matches the k8s defaults.
	LeaseDuration time.Duration

	// RenewDeadline is how long the leader has to renew before being
	// considered to have lost the lease. Must be < LeaseDuration.
	RenewDeadline time.Duration

	// RetryPeriod is how often clients re-try the acquire/renew loop.
	RetryPeriod time.Duration
}

// DefaultOptions returns the production-safe defaults. Override fields
// rather than rebuilding from scratch.
func DefaultOptions() Options {
	return Options{
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
	}
}

// Elector runs the lease loop and exposes IsLeader for the rest of the agent.
type Elector struct {
	isLeader atomic.Bool
	opts     Options
	client   kubernetes.Interface
	log      *slog.Logger

	// onAcquire and onLose run synchronously in the election goroutine — keep
	// them quick. Long-running work belongs in goroutines started from there.
	onAcquire func(ctx context.Context)
	onLose    func()
}

// New builds an Elector. The k8s client must already be authenticated and
// authorized to GET/CREATE/UPDATE coordination.k8s.io/v1.Lease in the
// configured namespace.
func New(client kubernetes.Interface, opts Options, log *slog.Logger) (*Elector, error) {
	if opts.LeaseName == "" {
		return nil, fmt.Errorf("LeaseName is required")
	}
	if opts.Namespace == "" {
		return nil, fmt.Errorf("Namespace is required")
	}
	if opts.Identity == "" {
		return nil, fmt.Errorf("Identity is required")
	}
	if opts.LeaseDuration == 0 {
		def := DefaultOptions()
		opts.LeaseDuration = def.LeaseDuration
		opts.RenewDeadline = def.RenewDeadline
		opts.RetryPeriod = def.RetryPeriod
	}

	return &Elector{
		opts:   opts,
		client: client,
		log:    log,
	}, nil
}

// OnAcquire sets the callback fired when this pod becomes leader. Use it
// to start the registrar + emitter loops.
func (e *Elector) OnAcquire(fn func(ctx context.Context)) { e.onAcquire = fn }

// OnLose sets the callback fired when this pod loses leadership (graceful
// shutdown, network partition, etc.). Use it to stop the leader-only work.
func (e *Elector) OnLose(fn func()) { e.onLose = fn }

// IsLeader reports whether this pod currently holds the lease.
func (e *Elector) IsLeader() bool { return e.isLeader.Load() }

// Run blocks until ctx is cancelled. It will repeatedly try to acquire the
// lease and call the OnAcquire/OnLose callbacks accordingly.
func (e *Elector) Run(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      e.opts.LeaseName,
			Namespace: e.opts.Namespace,
		},
		Client: e.client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: e.opts.Identity,
		},
	}

	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   e.opts.LeaseDuration,
		RenewDeadline:   e.opts.RenewDeadline,
		RetryPeriod:     e.opts.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				e.isLeader.Store(true)
				e.log.Info("became leader", "identity", e.opts.Identity)
				if e.onAcquire != nil {
					e.onAcquire(leaderCtx)
				}
			},
			OnStoppedLeading: func() {
				e.isLeader.Store(false)
				e.log.Info("lost leadership", "identity", e.opts.Identity)
				if e.onLose != nil {
					e.onLose()
				}
			},
			OnNewLeader: func(identity string) {
				if identity != e.opts.Identity {
					e.log.Info("standby — leader is somewhere else", "leader", identity)
				}
			},
		},
	}

	// Sanity check on the timing constants — k8s panics otherwise.
	if cfg.RenewDeadline >= cfg.LeaseDuration {
		return fmt.Errorf("RenewDeadline (%s) must be < LeaseDuration (%s)", cfg.RenewDeadline, cfg.LeaseDuration)
	}

	leaderelection.RunOrDie(ctx, cfg)
	return nil
}

// Acknowledge an import that staticcheck would otherwise flag — the
// coordinationv1 type is referenced indirectly through resourcelock.
var _ = coordinationv1.LeaseSpec{}
