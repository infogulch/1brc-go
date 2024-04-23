package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"unsafe"
)

const MEGABYTE = 1024 * 1024

var (
	BUFFER_SIZE int64 = 4 * MEGABYTE
	WORKERS           = runtime.NumCPU() * 2
)

type StationData struct {
	sum      int64
	count    int32
	min, max int16
}

func (t *StationData) add(x int16) {
	t.count += 1
	t.sum += int64(x)
	if t.min > x {
		t.min = x
	}
	if x > t.max {
		t.max = x
	}
}

func (t *StationData) merge(o *StationData) {
	t.count += o.count
	t.sum += o.sum
	if t.min > o.min {
		t.min = o.min
	}
	if o.max > t.max {
		t.max = o.max
	}
}

type StationMap map[string]*StationData

func (sm StationMap) Get(name []byte) *StationData {
	d, ok := sm[bytesToString(name)] // temporary read-only usage is safe
	if ok {
		return d
	} else {
		d := &StationData{min: math.MaxInt16, max: math.MinInt16}
		sm[string(name)] = d
		return d
	}
}

func main() {
	profile := flag.Bool("profile", false, "enable profiling")
	flag.Parse()
	if *profile {
		f, err := os.Create("default.pgo")
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	run("measurements.txt", "results.txt")
}

type job struct {
	jobid int64
	start int64
	head  <-chan []byte
	tail  chan<- []byte
}

func run(measureFile, outFile string) {

	jobs := make(chan job, WORKERS*4)
	results := make(chan StationMap, WORKERS)

	// ReaderAt allows multiple workers to read from the file in
	// parallel without conflicts
	var file io.ReaderAt

	// start workers
	for range WORKERS {
		go func() {
			// each worker owns its buffer, and accumulates data to its own map
			buf := make([]byte, BUFFER_SIZE)
			smap := make(StationMap)
			for job := range jobs {
				n, err := file.ReadAt(buf, job.start)
				if err != nil && err != io.EOF {
					panic(fmt.Errorf("failed to read file: %w", err))
				}

				first, last := process(buf[:n], smap)

				// last is a slice of buf which will be overwritten on next
				// iteration; send clone to next worker
				job.tail <- bytes.Clone(last)

				// reprocess the first line with the last line from the previous
				// worker
				first = slices.Concat([]byte{'\n'}, <-job.head, first, []byte{'\n'})
				process(first, smap)

				if job.jobid&(BUFFER_SIZE>>15-1) == 0 {
					log.Printf("completed job #%d at offset %dMB\n", job.jobid, job.start/MEGABYTE)
				}
			}
			results <- smap
		}()
	}

	// fill job queue
	{
		osfile := try(os.Open(measureFile))("open file")
		filesize := try(osfile.Stat())("stat file").Size()
		file = io.ReaderAt(osfile)
		head := make(chan []byte, 1)
		head <- nil
		var jobid, start int64
		for start < filesize {
			tail := make(chan []byte, 1)
			jobs <- job{jobid, start, head, tail}
			head = tail
			jobid++
			start += BUFFER_SIZE
		}
		close(jobs) // note: closing jobs causes workers to break only after all jobs are done.
	}

	// after a worker finishes it sends its map to results chan. accumulate map
	// data into a sorted list for printing.
	type StationEntry struct {
		Name string
		Data *StationData
	}
	result := make([]StationEntry, 0, 1000)
	for range WORKERS {
		for k, d := range <-results {
			e := StationEntry{k, d}
			i, found := slices.BinarySearchFunc(result, e, func(x, y StationEntry) int { return strings.Compare(x.Name, y.Name) })
			if !found {
				result = slices.Insert(result, i, e)
			} else {
				result[i].Data.merge(e.Data)
			}
		}
	}

	outf := try(os.OpenFile(outFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm))("open result file")
	outf.Seek(0, io.SeekStart)
	out := bufio.NewWriter(outf)
	defer out.Flush()
	out.WriteByte('{')
	sep := ""
	for _, e := range result {
		fmt.Fprintf(out, "%s%s=%.1f/%.1f/%.1f", sep, e.Name, float64(e.Data.min)/10, math.Round(float64(e.Data.sum)/float64(e.Data.count))/10, float64(e.Data.max)/10)
		sep = ", "
	}
	out.WriteString("}\n")
}

func process(b []byte, smap StationMap) ([]byte, []byte) {
	first, b, _ := bytes.Cut(b, []byte{'\n'})
	for len(b) > 0 {
		i := bytes.IndexByte(b, ';')
		if i < 0 {
			break
		}
		j := bytes.IndexByte(b[i:], '\n')
		if j < 0 {
			break
		}
		smap.Get(b[:i]).add(parseTemp(b[i+1 : i+j]))
		b = b[i+j+1:]
	}
	return first, b
}

func parseTemp(numb []byte) int16 {
	var num, sign int16 = 0, 1
	for _, c := range numb {
		switch c {
		case '.':
		case '-':
			sign = -1
		default:
			num = num*10 + int16(c-'0')
		}
	}
	return num * sign
}

func try[T any](t T, err error) func(string) T {
	return func(desc string) T {
		if err != nil {
			panic(fmt.Errorf("failed to %s: %w", desc, err))
		}
		return t
	}
}

// Implementation copied from strings.Builder
func bytesToString(buf []byte) string {
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}
