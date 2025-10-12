package interceptor_test

import (
	"errors"
	"testing"

	"github.com/litesql/go-ha"
	"github.com/litesql/ha/internal/interceptor"
)

func TestLoad(t *testing.T) {
	i, err := interceptor.Load("./testdata/ignore_alter_table_errors.go")
	if err != nil {
		t.Fatal(err)
	}
	cs := new(ha.ChangeSet)
	cs.AddChange(ha.Change{
		Command: "ALTER TABLE test",
	})
	err = i.AfterApply(cs, nil, errors.New("test"))
	if err != nil {
		t.Errorf("expect nil error, got %v", err)
	}
}
