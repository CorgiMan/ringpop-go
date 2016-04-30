package forward

import "github.com/CorgiMan/ringpop-go/events"
import "github.com/stretchr/testify/mock"

type EventListener struct {
	mock.Mock
}

// HandleEvent provides a mock function with given fields: event
func (_m *EventListener) HandleEvent(event events.Event) {
	_m.Called(event)
}
