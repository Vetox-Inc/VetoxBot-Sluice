package proxy

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const clientIdleTimeout = 10 * time.Minute

var (
	errQueueFull        = errors.New("rate-limit queue is full")
	errTooManyClients   = errors.New("client state limit reached")
	errBucketStateLimit = errors.New("rate-limit state capacity exhausted")
	errProxyClosed      = errors.New("proxy is shutting down")
)

type authType uint8

const (
	authBot authType = iota
	authNone
	authBearer
)

type identity struct {
	kind  authType
	key   [sha256.Size]byte
	botID string
	label string
}

func identify(authorization string) identity {
	authorization = strings.TrimSpace(authorization)
	if authorization == "" {
		return identity{kind: authNone, label: "NoAuth"}
	}

	separator := strings.IndexAny(authorization, " \t")
	var scheme, credential string
	if separator < 0 {
		scheme, credential = "Bot", authorization
	} else {
		scheme, credential = authorization[:separator], authorization[separator+1:]
		credential = strings.TrimSpace(credential)
	}

	kind := authBot
	switch {
	case strings.EqualFold(scheme, "Basic"):
		return identity{kind: authNone, label: "NoAuth"}
	case strings.EqualFold(scheme, "Bearer"):
		kind = authBearer
	case !strings.EqualFold(scheme, "Bot"):
		return identity{kind: authNone, label: "NoAuth"}
	}

	key := sha256.Sum256([]byte(credential))
	result := identity{
		kind: kind,
		key:  key,
	}
	if kind == authBearer {
		result.label = "Bearer"
		return result
	}

	result.botID = botIDFromToken(credential)
	result.label = "Bot"
	return result
}

func botIDFromToken(token string) string {
	encoded, _, _ := strings.Cut(token, ".")
	decoded, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		decoded, err = base64.RawURLEncoding.DecodeString(encoded)
	}
	if err != nil || !isNumericInput(string(decoded)) {
		return ""
	}
	return string(decoded)
}

func (i identity) fingerprint() string {
	return "sha256:" + hex.EncodeToString(i.key[:])
}

type gateWaiter struct {
	ready   chan struct{}
	element *list.Element
}

// fifoGate is a cancellation-safe, bounded FIFO semaphore with capacity one.
type fifoGate struct {
	mu         sync.Mutex
	held       bool
	maxWaiters int
	waiters    list.List
}

func newFIFOGate(maxWaiters int) fifoGate {
	return fifoGate{maxWaiters: maxWaiters}
}

func (g *fifoGate) acquire(ctx context.Context) error {
	g.mu.Lock()
	if !g.held {
		g.held = true
		g.mu.Unlock()
		if err := ctx.Err(); err != nil {
			g.release()
			return err
		}
		return nil
	}
	if g.waiters.Len() >= g.maxWaiters {
		g.mu.Unlock()
		return errQueueFull
	}

	waiter := &gateWaiter{ready: make(chan struct{})}
	waiter.element = g.waiters.PushBack(waiter)
	g.mu.Unlock()

	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			g.release()
			return err
		}
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		if waiter.element != nil {
			g.waiters.Remove(waiter.element)
			waiter.element = nil
			g.mu.Unlock()
			return ctx.Err()
		}
		g.mu.Unlock()
		g.release()
		return ctx.Err()
	}
}

func (g *fifoGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	front := g.waiters.Front()
	if front == nil {
		g.held = false
		return
	}
	waiter := front.Value.(*gateWaiter)
	g.waiters.Remove(front)
	waiter.element = nil
	close(waiter.ready)
}

type bucketState struct {
	gate fifoGate

	active   atomic.Int64
	lastUsed atomic.Int64

	mu      sync.Mutex
	readyAt time.Time
	wake    chan struct{}
}

func newBucketState(maxWaiters int) *bucketState {
	bucket := &bucketState{gate: newFIFOGate(maxWaiters), wake: make(chan struct{})}
	bucket.touch()
	return bucket
}

func (b *bucketState) touch() {
	b.lastUsed.Store(time.Now().UnixNano())
}

func (b *bucketState) acquire(ctx context.Context) error {
	b.active.Add(1)
	b.touch()
	if err := b.gate.acquire(ctx); err != nil {
		b.active.Add(-1)
		return err
	}
	return nil
}

func (b *bucketState) release() {
	b.touch()
	b.active.Add(-1)
	b.gate.release()
}

func (b *bucketState) wait(ctx context.Context, reserve time.Duration) (time.Duration, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		b.mu.Lock()
		readyAt := b.readyAt
		wake := b.wake
		b.mu.Unlock()
		delay := time.Until(readyAt)
		if delay <= 0 {
			return 0, nil
		}
		if deadline, ok := ctx.Deadline(); ok && delay+reserve >= time.Until(deadline) {
			return delay, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			continue
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return 0, ctx.Err()
		}
	}
}

func (b *bucketState) blockUntil(readyAt time.Time) {
	b.mu.Lock()
	if readyAt.After(b.readyAt) {
		b.readyAt = readyAt
		close(b.wake)
		b.wake = make(chan struct{})
	}
	b.mu.Unlock()
}

func (b *bucketState) merge(other *bucketState) {
	other.mu.Lock()
	readyAt := other.readyAt
	other.mu.Unlock()
	b.blockUntil(readyAt)
}

func (b *bucketState) idle(now time.Time) bool {
	blocked := b.blocked(now)
	return !blocked && b.active.Load() == 0 && now.Sub(time.Unix(0, b.lastUsed.Load())) > clientIdleTimeout
}

func (b *bucketState) blocked(now time.Time) bool {
	return b.retryAfter(now) > 0
}

func (b *bucketState) retryAfter(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.readyAt.After(now) {
		return 0
	}
	return b.readyAt.Sub(now)
}

const (
	clientUnknown int32 = iota
	clientValid
	clientInvalid
)

type clientState struct {
	mu      sync.RWMutex
	buckets map[uint64]*bucketState
	aliases map[uint64]uint64

	identity   identity
	global     *pacer
	validation fifoGate
	validity   atomic.Int32
	active     atomic.Int64
	lastUsed   atomic.Int64
	aliasLimit atomic.Bool
	maxWaiters int
	slots      *resourceBudget
}

func newClientState(identity identity, globalLimit uint, maxWaiters int, slots *resourceBudget) *clientState {
	state := &clientState{
		buckets:    make(map[uint64]*bucketState),
		aliases:    make(map[uint64]uint64),
		identity:   identity,
		global:     newPacer(globalLimit, maxWaiters),
		validation: newFIFOGate(maxWaiters),
		maxWaiters: maxWaiters,
		slots:      slots,
	}
	if identity.kind == authNone {
		state.validity.Store(clientValid)
	}
	state.touch()
	return state
}

func (s *clientState) touch() {
	s.lastUsed.Store(time.Now().UnixNano())
}

func (s *clientState) begin() {
	s.active.Add(1)
	s.touch()
}

func (s *clientState) end() {
	s.active.Add(-1)
	s.touch()
}

func (s *clientState) bucket(routeHash uint64) (*bucketState, error) {
	s.mu.RLock()
	canonical, aliased := s.aliases[routeHash]
	if !aliased {
		canonical = routeHash
	}
	bucket := s.buckets[canonical]
	s.mu.RUnlock()
	if bucket != nil {
		return bucket, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	canonical, aliased = s.aliases[routeHash]
	if !aliased {
		canonical = routeHash
	}
	if bucket = s.buckets[canonical]; bucket == nil {
		if !s.slots.reserve(1) {
			return nil, errBucketStateLimit
		}
		bucket = newBucketState(s.maxWaiters)
		s.buckets[canonical] = bucket
	}
	return bucket, nil
}

func (s *clientState) acquireBucket(ctx context.Context, routeHash uint64) (*bucketState, error) {
	bucket, err := s.bucket(routeHash)
	if err != nil {
		return nil, err
	}
	if err := bucket.acquire(ctx); err != nil {
		return nil, err
	}
	for {
		target, err := s.bucket(routeHash)
		if err != nil {
			bucket.release()
			return nil, err
		}
		if target == bucket {
			return bucket, nil
		}
		bucket.release()
		bucket = target
		if err := target.acquire(ctx); err != nil {
			return nil, err
		}
	}
}

func (s *clientState) learnBucket(routeHash uint64, discordBucket, majorKey string, current *bucketState) *bucketState {
	if discordBucket == "" {
		return current
	}
	canonical := HashCRC64(discordBucket + "\x00" + majorKey)

	s.mu.Lock()
	target := s.buckets[canonical]
	_, aliasExists := s.aliases[routeHash]
	deleteRoute := s.buckets[routeHash] == current && routeHash != canonical
	additions := 0
	if target == nil {
		additions++
		target = current
	}
	if !aliasExists {
		additions++
	}
	removals := 0
	if deleteRoute {
		removals++
	}
	if net := additions - removals; net > 0 && !s.slots.reserve(int64(net)) {
		if s.aliasLimit.CompareAndSwap(false, true) {
			logger.Warn("Rate-limit state capacity prevented shared-bucket alias learning")
		}
		s.mu.Unlock()
		return current
	}
	if target != current {
		target.merge(current)
	}
	if s.buckets[canonical] == nil {
		s.buckets[canonical] = target
	}
	if deleteRoute {
		delete(s.buckets, routeHash)
	}
	s.aliases[routeHash] = canonical
	if released := removals - additions; released > 0 {
		s.slots.release(int64(released))
	}
	s.mu.Unlock()
	return target
}

func (s *clientState) sweep(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	released := int64(0)
	for hash, bucket := range s.buckets {
		if !bucket.idle(now) {
			continue
		}
		delete(s.buckets, hash)
		released++
	}
	for route, canonical := range s.aliases {
		if _, exists := s.buckets[canonical]; !exists {
			delete(s.aliases, route)
			released++
		}
	}
	s.slots.release(released)
	if released > 0 {
		s.aliasLimit.Store(false)
	}
}

func (s *clientState) clear() {
	s.mu.Lock()
	s.slots.release(int64(len(s.buckets) + len(s.aliases)))
	s.buckets = make(map[uint64]*bucketState)
	s.aliases = make(map[uint64]uint64)
	s.mu.Unlock()
}

func (s *clientState) idle(now time.Time) bool {
	if s.global.blocked(now) || s.active.Load() != 0 || now.Sub(time.Unix(0, s.lastUsed.Load())) <= clientIdleTimeout {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, bucket := range s.buckets {
		if bucket.active.Load() != 0 || bucket.blocked(now) {
			return false
		}
	}
	return true
}

func (p *Proxy) globalLimit(identity identity) uint {
	if limit := p.globalOverrides[identity.botID]; limit != 0 {
		return limit
	}
	if limit := p.globalOverrides[identity.fingerprint()]; limit != 0 {
		return limit
	}
	return discordGlobalLimit
}

func (p *Proxy) client(identity identity) (*clientState, error) {
	p.clientsMu.Lock()
	defer p.clientsMu.Unlock()
	select {
	case <-p.ctx.Done():
		return nil, errProxyClosed
	default:
	}
	if identity.kind == authNone {
		if p.noAuth == nil {
			return nil, errProxyClosed
		}
		p.noAuth.begin()
		return p.noAuth, nil
	}

	states := p.bots
	if identity.kind == authBearer {
		states = p.bearers
	}
	state := states[identity.key]
	if state == nil && identity.kind == authBearer && len(states) >= p.config.MaxBearerClients {
		if !evictOldestIdle(time.Now(), p.bearers) {
			return nil, errTooManyClients
		}
	}
	if state == nil {
		if len(p.bots)+len(p.bearers) >= p.config.MaxClientStates && !evictOldestIdle(time.Now(), p.bots, p.bearers) {
			return nil, errTooManyClients
		}
		state = newClientState(identity, p.globalLimit(identity), p.config.MaxQueueDepth, p.bucketSlots)
		states[identity.key] = state
	}
	state.begin()
	return state, nil
}

func evictOldestIdle(now time.Time, groups ...map[[sha256.Size]byte]*clientState) bool {
	var oldestKey [sha256.Size]byte
	var oldestState *clientState
	var oldestGroup map[[sha256.Size]byte]*clientState
	for _, states := range groups {
		for key, candidate := range states {
			if !candidate.idle(now) {
				continue
			}
			if oldestState == nil || candidate.lastUsed.Load() < oldestState.lastUsed.Load() {
				oldestKey, oldestState, oldestGroup = key, candidate, states
			}
		}
	}
	if oldestState == nil {
		return false
	}
	oldestState.clear()
	delete(oldestGroup, oldestKey)
	return true
}

func (p *Proxy) sweepClients(now time.Time) {
	p.clientsMu.Lock()
	for key, state := range p.bots {
		if state.idle(now) {
			state.clear()
			delete(p.bots, key)
		}
	}
	for key, state := range p.bearers {
		if state.idle(now) {
			state.clear()
			delete(p.bearers, key)
		}
	}
	states := make([]*clientState, 0, len(p.bots)+len(p.bearers)+1)
	if p.noAuth != nil {
		states = append(states, p.noAuth)
	}
	for _, state := range p.bots {
		states = append(states, state)
	}
	for _, state := range p.bearers {
		states = append(states, state)
	}
	p.clientsMu.Unlock()
	for _, state := range states {
		if state != nil {
			state.sweep(now)
		}
	}
}
