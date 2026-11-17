package inbound

import (
	"context"
	"os"
	"slices"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.InboundManager = (*Manager)(nil)

type Manager struct {
	registry     adapter.InboundRegistry
	endpoint     adapter.EndpointManager
	access       sync.Mutex
	inbounds     []adapter.Inbound
	inboundByTag map[string]adapter.Inbound
	scope        *adapter.Scope
	running      bool
}

func NewManager(registry adapter.InboundRegistry, endpoint adapter.EndpointManager) *Manager {
	return &Manager{
		registry:     registry,
		endpoint:     endpoint,
		inboundByTag: make(map[string]adapter.Inbound),
	}
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.access.Lock()
	m.scope = scope
	if stage == adapter.StartStateStarted {
		m.running = true
	}
	inbounds := slices.Clone(m.inbounds)
	m.access.Unlock()
	for _, inbound := range inbounds {
		name := "inbound/" + inbound.Type() + "[" + inbound.Tag() + "]"
		err := scope.Start(name, inbound, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Inbounds() []adapter.Inbound {
	m.access.Lock()
	defer m.access.Unlock()
	return m.inbounds
}

func (m *Manager) Get(tag string) (adapter.Inbound, bool) {
	m.access.Lock()
	inbound, found := m.inboundByTag[tag]
	m.access.Unlock()
	if found {
		return inbound, true
	}
	return m.endpoint.Get(tag)
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	inbound, err := m.registry.Create(ctx, router, logger, tag, outboundType, options)
	if err != nil {
		return err
	}
	m.access.Lock()
	if _, loaded := m.inboundByTag[tag]; loaded {
		m.access.Unlock()
		return E.New("duplicate inbound tag: ", tag)
	}
	scope := m.scope
	running := m.running
	m.access.Unlock()
	if running {
		name := "inbound/" + inbound.Type() + "[" + tag + "]"
		for _, stage := range adapter.ListStartStages {
			err = scope.Start(name, inbound, stage)
			if err != nil {
				_ = scope.CloseComponent(inbound)
				return err
			}
		}
	}
	m.access.Lock()
	defer m.access.Unlock()
	if _, loaded := m.inboundByTag[tag]; loaded {
		if running {
			_ = scope.CloseComponent(inbound)
		}
		return E.New("duplicate inbound tag: ", tag)
	}
	m.inbounds = append(m.inbounds, inbound)
	m.inboundByTag[tag] = inbound
	return nil
}

func (m *Manager) Remove(tag string) error {
	m.access.Lock()
	inbound, found := m.inboundByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.inboundByTag, tag)
	m.inbounds = slices.DeleteFunc(m.inbounds, func(it adapter.Inbound) bool {
		return it == inbound
	})
	scope := m.scope
	m.access.Unlock()
	if scope == nil {
		return nil
	}
	return scope.CloseComponent(inbound)
}
