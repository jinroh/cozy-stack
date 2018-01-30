package utils

import (
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomString(t *testing.T) {
	rand.Seed(42)
	s1 := RandomString(10)
	s2 := RandomString(20)

	rand.Seed(42)
	s3 := RandomString(10)
	s4 := RandomString(20)

	assert.Len(t, s1, 10)
	assert.Len(t, s2, 20)
	assert.Len(t, s3, 10)
	assert.Len(t, s4, 20)

	assert.NotEqual(t, s1, s2)
	assert.Equal(t, s1, s3)
	assert.Equal(t, s2, s4)
}

func TestRandomStringConcurrentAccess(t *testing.T) {
	n := 10000
	var wg sync.WaitGroup
	wg.Add(n)

	ms := make(map[string]struct{})
	var mu sync.Mutex

	var gotDup = false

	for i := 0; i < n; i++ {
		go func() {
			s := RandomString(10)
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			if _, ok := ms[s]; ok {
				gotDup = true
			}
			var q struct{}
			ms[s] = q
		}()
	}
	wg.Wait()

	if gotDup {
		t.Fatal("should be unique strings")
	}
}

func TestStripPort(t *testing.T) {
	d1 := StripPort("localhost")
	assert.Equal(t, "localhost", d1)
	d2 := StripPort("localhost:8080")
	assert.Equal(t, "localhost", d2)
	d3 := StripPort("localhost:8080:8081")
	assert.Equal(t, "localhost:8080:8081", d3)
}

func TestSplitTrimString(t *testing.T) {
	{
		parts := SplitTrimString("", ',')
		assert.EqualValues(t, []string{}, parts)
	}
	{
		parts := SplitTrimString("foo,bar,baz,", ',')
		assert.EqualValues(t, []string{"foo", "bar", "baz"}, parts)
	}
	{
		parts := SplitTrimString(",,,,", ',')
		assert.EqualValues(t, []string{}, parts)
	}
	{
		parts := SplitTrimString("foo  ,, bar,  baz  日本語,", ',')
		assert.EqualValues(t, []string{"foo", "bar", "baz  日本語"}, parts)
	}
	{
		parts := SplitTrimString("日, foo, 本, 語", ',')
		assert.EqualValues(t, []string{"日", "foo", "本", "語"}, parts)
	}
	{
		parts := SplitTrimString("日, foo, 本 , 語  ", ',')
		assert.EqualValues(t, []string{"日", "foo", "本", "語"}, parts)
	}
	{
		parts := SplitTrimString("日,本,語 ,", ',')
		assert.EqualValues(t, []string{"日", "本", "語"}, parts)
	}
	{
		parts := SplitTrimString("    ", ',')
		assert.EqualValues(t, []string{}, parts)
	}
	{
		parts := SplitTrimString(" foo   ", ',')
		assert.EqualValues(t, []string{"foo"}, parts)
	}
	{
		parts := SplitTrimString(" foo bar,", ',')
		assert.EqualValues(t, []string{"foo bar"}, parts)
	}
}

func TestFileExists(t *testing.T) {
	exists, err := FileExists("/no/such/file")
	assert.NoError(t, err)
	assert.False(t, exists)
	exists, err = FileExists("/etc/hosts")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestAbsPath(t *testing.T) {
	home := UserHomeDir()
	assert.NotEmpty(t, home)
	assert.Equal(t, home, AbsPath("~"))
	foo := AbsPath("foo")
	wd, _ := os.Getwd()
	assert.Equal(t, wd+"/foo", foo)
	bar := AbsPath("~/bar")
	assert.Equal(t, home+"/bar", bar)
	baz := AbsPath("$HOME/baz")
	assert.Equal(t, home+"/baz", baz)
	qux := AbsPath("/qux")
	assert.Equal(t, "/qux", qux)
	quux := AbsPath("////qux//quux/../quux")
	assert.Equal(t, "/qux/quux", quux)
}
