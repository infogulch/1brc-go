package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"unsafe"
)

const (
	BUFFER_SIZE = 4 * 1024 * 1024
	GOROUTINES  = 8
)

type job struct {
	jobid int64
	start int64
	head  <-chan []byte
	tail  chan<- []byte
}

// 16 bytes
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

type StationEntry struct {
	Name string
	Data *StationData
}

type StationMap map[string]*StationData

func (sm StationMap) Get(name []byte) *StationData {
	d, ok := sm[bytesToString(name)] // temporary read-only usage is safe
	if ok {
		return d
	} else {
		d := &StationData{}
		sm[string(name)] = d
		return d
	}
}

func main() {
	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	osfile := try(os.Open("measurements.txt"))("open file")
	filesize := try(osfile.Stat())("stat file").Size()
	file := io.ReaderAt(osfile)

	jobs := make(chan job, 100)
	results := make(chan StationMap, GOROUTINES)

	for range GOROUTINES {
		go func() {
			buf := make([]byte, BUFFER_SIZE)
			smap := make(StationMap)
			for job := range jobs {
				n, err := file.ReadAt(buf, job.start)
				if err != nil && err != io.EOF {
					try0(err, "read file")
				}

				first, last := process(buf[:n], smap)

				// last is a slice of buf which will be overwritten on next iteration; send clone
				job.tail <- bytes.Clone(last)

				// reprocess the first line with the last line from the previous map
				process(append(<-job.head, first...), smap)

				if job.jobid&(127) == 0 {
					fmt.Printf("completed job #%d at offset %d\n", job.jobid, job.start)
				}
			}
			results <- smap
		}()
	}

	{
		numjobs := int64(0)
		head := make(chan []byte, 1)
		head <- nil
		for start := int64(0); start < filesize; start += BUFFER_SIZE {
			numjobs += 1
			tail := make(chan []byte, 1)
			jobs <- job{numjobs, start, head, tail}
			head = tail
		}
		close(jobs)
		// fmt.Printf("started %d jobs\n", numjobs)
	}

	result := make([]StationEntry, 0, 1000)
	for range GOROUTINES {
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

	outf := try(os.OpenFile("results.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm))("open result file")
	outf.Seek(0, io.SeekStart)
	out := bufio.NewWriter(outf)
	for _, e := range result {
		try(fmt.Fprintf(out, "%s=%.1f/%.1f/%.1f,\n", e.Name, float64(e.Data.min)/10, float64(e.Data.sum)/float64(e.Data.count)/10, float64(e.Data.max)/10))("write result line")
	}
	try0(out.Flush(), "flush output")

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
		try0(err, desc)
		return t
	}
}

func try0(err error, desc string) {
	if err != nil {
		panic(fmt.Errorf("failed to %s: %w", desc, err))
	}
}

// Implementation copied from strings.Builder
func bytesToString(buf []byte) string {
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}
