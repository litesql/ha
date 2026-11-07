package interceptor

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/litesql/go-ha"
	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

//go:generate go run github.com/traefik/yaegi/cmd/yaegi extract github.com/litesql/go-ha
var Symbols = stdlib.Symbols

type beforeFn func(changeSet *ha.ChangeSet, conn *sql.Conn) (bool, error)

type afterFn func(changeSet *ha.ChangeSet, conn *sql.Conn, err error) error

func Load(filename string) (ha.ChangeSetInterceptor, error) {
	src, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	i := interp.New(interp.Options{})
	i.Use(Symbols)
	_, err = i.Eval(string(src))
	if err != nil {
		return nil, err
	}

	var (
		before beforeFn
		after  afterFn
		ok     bool
	)
	beforeReflect, err := i.Eval("ha.Before")
	if err == nil {
		before, ok = beforeReflect.Interface().(func(changeSet *ha.ChangeSet, conn *sql.Conn) (bool, error))
		if !ok {
			return nil, fmt.Errorf("invalid ha.Before signature")
		}
	}

	afterReflect, err := i.Eval("ha.After")
	if err == nil {
		after, ok = afterReflect.Interface().(func(changeSet *ha.ChangeSet, conn *sql.Conn, err error) error)
		if !ok {
			return nil, fmt.Errorf("invalid ha.After signature")
		}
	}

	ic := newInterceptor(before, after)
	if ic == nil {
		return nil, fmt.Errorf("interceptor script must define ha.Before or ha.After")
	}
	return ic, nil
}

func newInterceptor(before beforeFn, after afterFn) *baseInterceptor {
	if before == nil && after == nil {
		return nil
	}
	if before == nil {
		before = noopBefore
	}
	if after == nil {
		after = noopAfter
	}
	return &baseInterceptor{
		before: before,
		after:  after,
	}
}

type baseInterceptor struct {
	mu     sync.Mutex
	before beforeFn
	after  afterFn
}

func (i *baseInterceptor) BeforeApply(cs *ha.ChangeSet, conn *sql.Conn) (skip bool, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.before(cs, conn)
}
func (i *baseInterceptor) AfterApply(cs *ha.ChangeSet, conn *sql.Conn, err error) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.after(cs, conn, err)
}

func noopBefore(cs *ha.ChangeSet, conn *sql.Conn) (bool, error) {
	return false, nil
}

func noopAfter(cs *ha.ChangeSet, conn *sql.Conn, err error) error {
	return err
}
