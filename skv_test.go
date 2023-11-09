package skv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestGetWithPrefix(t *testing.T) {
	os.RemoveAll("skv-test.db")
	prefix := "RandomPrefix-"
	db, err := Open[string]("skv-test.db")
	if err != nil {
		t.Fatal(err)
	}
	// put a key
	if err := db.Put(prefix+"key1", "value1"); err != nil {
		t.Fatal(err)
	}
	// get it back
	val, err := db.Get(prefix + "key1")
	if err != nil {
		t.Fatal(err)
	} else if val != "value1" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}
	// put something else in and get it
	if err := db.Put("key122", "value122"); err != nil {
		t.Fatal(err)
	}
	val, err = db.Get("key122")
	if err != nil {
		t.Fatal(err)
	} else if val != "value122" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}

	// put something else in (within the prefix) and get it
	if err := db.Put(prefix+"key2", "value3"); err != nil {
		t.Fatal(err)
	}
	val, err = db.Get(prefix + "key2")
	if err != nil {
		t.Fatal(err)
	} else if val != "value3" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}

	result := []string{
		"value1",
		"value3",
	}
	resultBytes, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}

	prefixVal, err := db.GetWithPrefix(prefix)
	if err != nil {
		t.Fatal(err)
	}

	prefixValBytes, err := json.Marshal(prefixVal)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(prefixValBytes, resultBytes) {
		t.Fatal("differences encounted")
	}

	// done
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBasic(t *testing.T) {
	os.RemoveAll("skv-test.db")
	db, err := Open[string]("skv-test.db")
	if err != nil {
		t.Fatal(err)
	}
	// put a key
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	// get it back
	val, err := db.Get("key1")
	if err != nil {
		t.Fatal(err)
	} else if val != "value1" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}
	// put it again with same value
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	// get it back again
	val, err = db.Get("key1")
	if err != nil {
		t.Fatal(err)
	} else if val != "value1" {
		t.Fatalf("got \"%s\", expected \"value1\"", val)
	}
	// get something we know is not there
	val, err = db.Get("no.such.key")
	if err != ErrNotFound {
		t.Fatalf("got \"%s\", expected absence", val)
	}
	// delete our key
	if err := db.Delete("key1"); err != nil {
		t.Fatal(err)
	}
	// done
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMoreNotFoundCases(t *testing.T) {
	os.RemoveAll("skv-test.db")
	db, err := Open[string]("skv-test.db")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Get("key1")
	if err != ErrNotFound {
		t.Fatal(err)
	}
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("key1"); err != nil {
		t.Fatal(err)
	}
	_, err = db.Get("key1")
	if err != ErrNotFound {
		t.Fatal(err)
	}
	_, err = db.Get("")
	if err != ErrNotFound {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRichTypes(t *testing.T) {
	var inval1 = map[string]string{
		"100 meters": "Florence GRIFFITH-JOYNER",
		"200 meters": "Florence GRIFFITH-JOYNER",
		"400 meters": "Marie-José PÉREC",
		"800 meters": "Nadezhda OLIZARENKO",
	}
	testGetPut(t, inval1)
}

func testGetPut(t *testing.T, inval map[string]string) {
	os.RemoveAll("skv-test.db")
	db, err := Open[map[string]string]("skv-test.db")
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(inval)
	if err != nil {
		t.Fatal(err)
	}

	var outval map[string]string
	if err := db.Put("test.key", inval); err != nil {
		t.Fatal(err)
	}
	outval, err = db.Get("test.key")
	if err != nil {
		t.Fatal(err)
	}
	output, err := json.Marshal(outval)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, output) {
		t.Fatal("differences encountered")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGoroutines(t *testing.T) {
	os.RemoveAll("skv-test.db")
	db, err := Open[string]("skv-test.db")
	if err != nil {
		t.Fatal(err)
	}
	rand.Seed(time.Now().UnixNano())
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			switch rand.Intn(3) {
			case 0:
				if err := db.Put("key1", "value1"); err != nil {
					t.Fatal(err)
				}
			case 1:
				_, err := db.Get("key1")
				if err != nil && err != ErrNotFound {
					t.Fatal(err)
				}
			case 2:
				if err := db.Delete("key1"); err != nil && err != ErrNotFound {
					t.Fatal(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	db.Close()
}

func TestGetKeys(t *testing.T) {
	os.RemoveAll("skv-test.db")
	db, err := Open[string]("skv-test.db")
	if err != nil {
		t.Fatal(err)
	}
	if l, err := db.GetKeys(); err != nil || len(l) != 0 {
		t.Fatal("GetKey slice should be empty")
	}
	if err := db.Put("test.key", "TESTVALUE"); err != nil {
		t.Fatal(err)
	}
	if l, err := db.GetKeys(); err != nil || len(l) != 1 {
		t.Fatal("GetKey slice should contain one key")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	os.RemoveAll("skv-bench.db")
	db, err := Open[string]("skv-bench.db")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
}

func BenchmarkPutGet(b *testing.B) {
	os.RemoveAll("skv-bench.db")
	db, err := Open[string]("skv-bench.db")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(fmt.Sprintf("key%d", i)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
}

func BenchmarkPutDelete(b *testing.B) {
	os.RemoveAll("skv-bench.db")
	db, err := Open[string]("skv-bench.db")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < b.N; i++ {
		if err := db.Delete(fmt.Sprintf("key%d", i)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
}
