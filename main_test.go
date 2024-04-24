package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestSamples(t *testing.T) {
	log.SetOutput(io.Discard)
	WORKERS = 1
	paths := try(filepath.Glob("./samples/*.txt"))("glob sample files")
	for _, path := range paths {
		t.Run("full:"+path, func(t *testing.T) {
			prefix := path[:len(path)-len(filepath.Ext(path))]

			actualPath := prefix + ".actual.out"
			run(path, actualPath)
			actual := try(os.ReadFile(actualPath))("read actual file")

			expectedPath := prefix + ".out"
			expected := try(os.ReadFile(expectedPath))("read expected file")

			if !bytes.Equal(expected, actual) {
				t.Errorf("expected did not match actual")
			}
		})
	}
	BUFFER_SIZE = 150
	for _, path := range paths {
		t.Run("smallbuf:"+path, func(t *testing.T) {
			prefix := path[:len(path)-len(filepath.Ext(path))]

			actualPath := prefix + ".actual.out"
			run(path, actualPath)
			actual := try(os.ReadFile(actualPath))("read actual file")

			expectedPath := prefix + ".out"
			expected := try(os.ReadFile(expectedPath))("read expected file")

			if !bytes.Equal(expected, actual) {
				t.Errorf("expected did not match actual")
			}
		})
	}
}
