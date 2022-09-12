package iohelpers_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/medical_claims_tools/internal/iohelpers"
)

func TestMultiWriteCloser(t *testing.T) {
	b1 := &memoryWriteCloser{}
	b2 := &memoryWriteCloser{}
	data := []byte(`some data!`)

	mwc := iohelpers.MultiWriteCloser{}
	mwc.Add(b1)
	mwc.Add(b2)
	mwc.Write(data)

	// Test that both in memory write closers got data written.
	if b1Data := b1.Data(); !cmp.Equal(b1Data, data) {
		t.Errorf("MultiWriteCloser: unexpected data written. got: %s, want: %s", b1Data, data)
	}
	if b2Data := b2.Data(); !cmp.Equal(b2Data, data) {
		t.Errorf("MultiWriteCloser: unexpected data written. got: %s, want: %s", b2Data, data)
	}

	// Test nil error on close:
	if err := mwc.Close(); err != nil {
		t.Errorf("MultiWriteCloser.Close(): unexpected error. got: %v, want: nil", err)
	}
}

func TestMultiWriteCloser_Error(t *testing.T) {
	e1 := errors.New("error 1")
	b1 := &memoryWriteCloser{
		ErrToReturnOnClose: e1,
	}

	e2 := errors.New("error 2")
	b2 := &memoryWriteCloser{
		ErrToReturnOnClose: e2,
	}
	data := []byte(`some data!`)

	mwc := iohelpers.MultiWriteCloser{}
	mwc.Add(b1)
	mwc.Add(b2)
	mwc.Write(data)

	// Test that both in memory write closers got data written.
	if b1Data := b1.Data(); !cmp.Equal(b1Data, data) {
		t.Errorf("MultiWriteCloser: unexpected data written. got: %s, want: %s", b1Data, data)
	}
	if b2Data := b2.Data(); !cmp.Equal(b2Data, data) {
		t.Errorf("MultiWriteCloser: unexpected data written. got: %s, want: %s", b2Data, data)
	}

	// Test errors on close
	mwcErr := mwc.Close()
	if mwcErr == nil {
		t.Errorf("MultiWriteCloser.Close(): unexpected error. got: %v, want: non-nil", mwcErr)
	}
	if !strings.Contains(mwcErr.Error(), e1.Error()) {
		t.Errorf("MultiWriteCloser.Close() expected error to contain err substring. got: %v, expected substring: %v", mwcErr, e1)
	}
	if !strings.Contains(mwcErr.Error(), e2.Error()) {
		t.Errorf("MultiWriteCloser.Close() expected error to contain err substring. got: %v, expected substring: %v", mwcErr, e2)
	}
}

type memoryWriteCloser struct {
	ErrToReturnOnClose error
	Buf                bytes.Buffer
}

func (m *memoryWriteCloser) Write(p []byte) (n int, err error) {
	return m.Buf.Write(p)
}

func (m *memoryWriteCloser) Close() error {
	return m.ErrToReturnOnClose
}

func (m *memoryWriteCloser) Data() []byte {
	return m.Buf.Bytes()
}
