package interceptor_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
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

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = i.BeforeApply(cs, nil)
			_ = i.AfterApply(cs, nil, errors.New("test"))
		}()
	}
	wg.Wait()
}

func TestLoadWithoutFunctionsReturnsError(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "empty.go")
	if err := os.WriteFile(tmp, []byte("package ha\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	i, err := interceptor.Load(tmp)
	if err == nil {
		t.Fatalf("expected error when neither Before nor After is defined, got interceptor=%v", i)
	}
}
