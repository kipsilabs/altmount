package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
)

// ErrProcessingCancelled identifies an explicit user cancellation, as opposed
// to service shutdown, which leaves interrupted imports resumable on restart.
var ErrProcessingCancelled = errors.New("processing cancelled by user request")

// QueueEventListener receives notifications about queue item lifecycle events.
type QueueEventListener interface {
	OnItemClaimed(ctx context.Context, item *database.ImportQueueItem)
}

// ItemProcessor defines the interface for processing queue items
type ItemProcessor interface {
	// ProcessItem processes a single queue item and returns the resulting path or an error
	ProcessItem(ctx context.Context, item *database.ImportQueueItem) (string, error)
	// HandleSuccess handles successful processing
	HandleSuccess(ctx context.Context, item *database.ImportQueueItem, resultingPath string) error
	// HandleFailure handles failed processing
	HandleFailure(ctx context.Context, item *database.ImportQueueItem, err error)
}

// ManagerConfig holds configuration for the queue manager
type ManagerConfig struct {
	Workers      int
	ConfigGetter config.ConfigGetter
}

// Manager manages queue workers and processing
type Manager struct {
	config       ManagerConfig
	repository   *database.QueueRepository
	claimer      *Claimer
	processor    ItemProcessor
	listener     QueueEventListener
	configGetter config.ConfigGetter
	log          *slog.Logger

	// Runtime state
	mu      sync.RWMutex
	running bool
	paused  bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Per-worker loop-control contexts (separate from m.ctx used for item processing).
	// Cancelling a worker's loopCancel stops its ticker loop without cancelling in-flight items.
	workerCancels []context.CancelFunc
	workerCount   int

	// claimMu serialises DB claim transactions to avoid SQLite lock contention.
	claimMu sync.Mutex

	// notifyChan is used to signal workers when new items arrive in the queue
	notifyChan chan struct{}

	// Cancellation tracking for processing items
	cancelFuncs map[int64]context.CancelFunc
	cancelMu    sync.RWMutex
}

// NewManager creates a new queue manager
func NewManager(cfg ManagerConfig, repository *database.QueueRepository, processor ItemProcessor, listener QueueEventListener) *Manager {
	if cfg.Workers == 0 {
		cfg.Workers = 2
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		config:       cfg,
		repository:   repository,
		claimer:      NewClaimer(repository),
		processor:    processor,
		listener:     listener,
		configGetter: cfg.ConfigGetter,
		log:          slog.Default().With("component", "queue-manager"),
		ctx:          ctx,
		cancel:       cancel,
		cancelFuncs:  make(map[int64]context.CancelFunc),
		notifyChan:   make(chan struct{}, 10),
	}
}

// Start starts the queue workers
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	// Start worker pool with per-worker loop contexts
	m.workerCancels = make([]context.CancelFunc, 0, m.config.Workers)
	for i := 0; i < m.config.Workers; i++ {
		loopCtx, loopCancel := context.WithCancel(m.ctx)
		m.workerCancels = append(m.workerCancels, loopCancel)
		m.wg.Add(1)
		go m.workerLoop(i, loopCtx)
	}
	m.workerCount = m.config.Workers

	m.running = true
	m.log.InfoContext(ctx, "Queue manager started", "workers", m.config.Workers)

	return nil
}

// Stop stops the queue workers
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()

	if !m.running {
		m.mu.Unlock()
		return nil
	}

	m.log.InfoContext(ctx, "Stopping queue manager")

	// Cancel all worker loop contexts and the manager context
	for _, cancel := range m.workerCancels {
		cancel()
	}
	m.cancel()
	m.running = false
	m.workerCancels = nil
	m.workerCount = 0
	m.mu.Unlock()

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines finished
	case <-time.After(30 * time.Second):
		m.log.WarnContext(ctx, "Timeout waiting for workers to stop")
	case <-ctx.Done():
		m.log.WarnContext(ctx, "Context cancelled while waiting for workers")
		return ctx.Err()
	}

	// Re-acquire lock to recreate context for potential restart
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.log.InfoContext(ctx, "Queue manager stopped")
	return nil
}

// Pause pauses queue processing
func (m *Manager) Pause() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paused = true
	m.log.InfoContext(m.ctx, "Queue manager paused")
}

// Resume resumes queue processing
func (m *Manager) Resume() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paused = false
	m.log.InfoContext(m.ctx, "Queue manager resumed")
}

// IsPaused returns whether the manager is paused
func (m *Manager) IsPaused() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.paused
}

// IsRunning returns whether the manager is running
func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// CancelProcessing cancels processing for a specific item
func (m *Manager) CancelProcessing(itemID int64) error {
	m.cancelMu.RLock()
	cancel, exists := m.cancelFuncs[itemID]
	m.cancelMu.RUnlock()

	if !exists {
		return nil // Not currently processing
	}

	m.log.InfoContext(m.ctx, "Cancelling processing for queue item", "item_id", itemID)
	cancel()
	return nil
}

// ExecuteItem manually triggers processing for a specific queue item, bypassing concurrency limits.
func (m *Manager) ExecuteItem(ctx context.Context, itemID int64) error {
	item, err := m.repository.GetQueueItem(ctx, itemID)
	if err != nil {
		return err
	}
	if item == nil {
		return fmt.Errorf("queue item %d not found", itemID)
	}

	// Set status to processing before running
	err = m.repository.UpdateQueueItemStatus(ctx, itemID, database.QueueStatusProcessing, nil)
	if err != nil {
		return err
	}

	m.log.InfoContext(ctx, "Manually triggering processing for queue item", "queue_id", itemID)

	go func() {
		// Use a separate context for the execution to avoid early termination
		itemCtx, cancelItem := context.WithCancelCause(context.Background())
		defer cancelItem(nil)
		cancel := func() { cancelItem(ErrProcessingCancelled) }
		m.cancelMu.Lock()
		m.cancelFuncs[item.ID] = cancel
		m.cancelMu.Unlock()

		defer func() {
			m.cancelMu.Lock()
			delete(m.cancelFuncs, item.ID)
			m.cancelMu.Unlock()
		}()

		resultingPath, processingErr := m.processor.ProcessItem(itemCtx, item)

		if processingErr != nil {
			m.processor.HandleFailure(itemCtx, item, processingErr)
		} else {
			m.processor.HandleSuccess(itemCtx, item, resultingPath)
		}
	}()

	return nil
}

// Resize dynamically adjusts the number of queue workers.
// When scaling down, excess workers finish their current item before stopping.
func (m *Manager) Resize(ctx context.Context, newCount int) error {
	if newCount <= 0 {
		return fmt.Errorf("worker count must be positive, got %d", newCount)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		// Not running — just update config for next Start()
		m.config.Workers = newCount
		return nil
	}

	oldCount := m.workerCount
	if newCount == oldCount {
		return nil
	}

	if newCount > oldCount {
		// Scale up: start additional workers
		for i := oldCount; i < newCount; i++ {
			loopCtx, loopCancel := context.WithCancel(m.ctx)
			m.workerCancels = append(m.workerCancels, loopCancel)
			m.wg.Add(1)
			go m.workerLoop(i, loopCtx)
		}
	} else {
		// Scale down: cancel excess worker loop contexts
		for i := newCount; i < oldCount; i++ {
			m.workerCancels[i]()
		}
		m.workerCancels = m.workerCancels[:newCount]
	}

	m.workerCount = newCount
	m.config.Workers = newCount
	m.log.InfoContext(ctx, "Queue workers resized", "old_count", oldCount, "new_count", newCount)
	return nil
}

// NotifyNewItem signals queue workers that a new item is available for immediate processing.
func (m *Manager) NotifyNewItem() {
	select {
	case m.notifyChan <- struct{}{}:
	default:
	}
}

// workerLoop is the main worker loop.
// loopCtx controls this worker's lifecycle (cancelled on Resize shrink or Stop).
// Item processing uses m.ctx so in-flight items are not cancelled when a worker is removed by Resize.
func (m *Manager) workerLoop(workerID int, loopCtx context.Context) {
	defer m.wg.Done()

	log := m.log.With("worker_id", workerID)

	// Get processing interval from configuration
	processingIntervalSeconds := m.configGetter().Import.QueueProcessingIntervalSeconds
	processingInterval := time.Duration(processingIntervalSeconds) * time.Second

	ticker := time.NewTicker(processingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if m.IsPaused() {
				continue
			}
			for m.processNextItem(m.ctx, workerID) {
				if m.IsPaused() {
					break
				}
			}
		case <-m.notifyChan:
			if m.IsPaused() {
				continue
			}
			for m.processNextItem(m.ctx, workerID) {
				if m.IsPaused() {
					break
				}
			}
		case <-loopCtx.Done():
			log.Info("Queue worker stopped")
			return
		}
	}
}

// processNextItem claims and processes the next queue item.
// Returns true if an item was claimed and processed, false if no item was claimed or an error occurred.
func (m *Manager) processNextItem(ctx context.Context, workerID int) bool {
	m.claimMu.Lock()
	item, err := m.claimer.ClaimWithRetry(ctx, workerID)
	m.claimMu.Unlock()

	if err != nil {
		if !IsDatabaseContentionError(err) {
			m.log.ErrorContext(ctx, "Failed to claim next queue item", "worker_id", workerID, "error", err)
		}
		return false
	}

	if item == nil {
		return false // No work to do
	}

	if m.listener != nil {
		m.listener.OnItemClaimed(ctx, item)
	}

	m.log.DebugContext(ctx, "Processing claimed queue item", "worker_id", workerID, "queue_id", item.ID, "file", item.NzbPath)

	itemCtx, cancelItem := context.WithCancelCause(ctx)
	defer cancelItem(nil)
	cancel := func() { cancelItem(ErrProcessingCancelled) }
	m.cancelMu.Lock()
	m.cancelFuncs[item.ID] = cancel
	m.cancelMu.Unlock()

	defer func() {
		m.cancelMu.Lock()
		delete(m.cancelFuncs, item.ID)
		m.cancelMu.Unlock()
	}()

	resultingPath, processingErr := m.processor.ProcessItem(itemCtx, item)

	if processingErr != nil {
		m.processor.HandleFailure(itemCtx, item, processingErr)
	} else {
		m.processor.HandleSuccess(ctx, item, resultingPath)
	}

	return true
}
