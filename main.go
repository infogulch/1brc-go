package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"runtime/pprof"
	"slices"
	"sync"
)

const (
	BUFFER_SIZE = 4 * 1024 * 1024
	GOROUTINES  = 4
	MAPLEN      = 1 << 15 // with max 10k station names, this remains under 50% fill rate
)

type job struct {
	jobid int64
	start int64
	head  <-chan []byte
	tail  chan<- []byte
}

// 24 bytes
// 24 * MAPLEN = 786kb
type StationData struct {
	sum      int64
	count    int32
	min, max int16
	otheridx uint16
	namelen  uint16
	nameidx  uint32
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

func (t *StationData) merge(u *StationData) {
	t.sum += u.sum
	t.count += u.count
	if u.min < t.min {
		t.min = u.min
	}
	if u.max > t.max {
		t.max = u.max
	}
	if t.nameidx == 0 {
		t.nameidx = u.nameidx
		t.namelen = u.namelen
	}
}

func (t *StationData) name() []byte {
	return namesBuf[t.nameidx : t.nameidx+uint32(t.namelen)]
}

type StationMap []StationData

func (sm StationMap) Get(name []byte) *StationData {
	i1, i2 := hash2(name)
	d1, d2 := &sm[i1], &sm[i2]
	if d1.otheridx == i2 {
		// debug: check name
		return d1
	}
	if d2.otheridx == i1 {
		// debug: check name
		return d2
	}
	nameidx := getNameIdx(name)
	namelen := uint16(len(name))
	if d1.nameidx == 0 {
		d1.nameidx = nameidx
		d1.namelen = namelen
		d1.otheridx = i2
		return d1
	}
	if d2.nameidx == 0 {
		d2.nameidx = nameidx
		d2.namelen = namelen
		d2.otheridx = i1
		return d2
	}
	if s := &sm[d1.otheridx]; s.nameidx == 0 {
		*d1, *s = *s, *d1
		d1.otheridx, s.otheridx = s.otheridx, d1.otheridx
		d1.nameidx = nameidx
		d1.namelen = namelen
		d1.otheridx = i2
		return d1
	}
	if s := &sm[d2.otheridx]; s.nameidx == 0 {
		*d2, *s = *s, *d2
		d2.otheridx, s.otheridx = s.otheridx, d2.otheridx
		d2.nameidx = nameidx
		d2.namelen = namelen
		d2.otheridx = i1
		return d2
	}
	panic("double cuckoo move not implemented")
}

var namesLock sync.RWMutex
var namesBuf = make([]byte, 1, 100*10000)
var namesMap = make(map[uint64]uint32)

func getNameIdx(name []byte) uint32 {
	h := hash(name)
	namesLock.RLock()
	if i, ok := namesMap[h]; ok {
		namesLock.RUnlock()
		return i
	}
	namesLock.RUnlock()
	namesLock.Lock()
	if i, ok := namesMap[h]; ok {
		namesLock.Unlock()
		return i
	}
	i := uint32(len(namesBuf))
	namesBuf = append(namesBuf, name...)
	namesMap[h] = i
	namesLock.Unlock()
	return i
}

// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
// https://github.com/skeeto/hash-prospector
func hash(name []byte) uint64 {
	var x, y uint64 = 0xd6e8feb86659fd93, 0xd6e8feb86659fd93
	for _, c := range name {
		x ^= x<<3 + uint64(c)
		y ^= (y>>11 | y<<53) + uint64(c)
	}
	x ^= y
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func hash2(name []byte) (uint16, uint16) {
	x := hash(name)
	return uint16(x >> 32 * MAPLEN >> 32), uint16(x & math.MaxUint32 * MAPLEN >> 32)
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
			smap := make(StationMap, MAPLEN)
			for job := range jobs {
				n, err := file.ReadAt(buf, job.start)
				if err != nil && err != io.EOF {
					try0(err, "read file")
				}

				first, last := process(buf[:n], smap)
				job.tail <- last
				process(append(<-job.head, first...), smap)

				if job.jobid&(127) == 0 {
					fmt.Printf("completed job #%d at offset %d\n", job.jobid, job.start)
				}
			}
			results <- smap
		}()
	}

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
	fmt.Printf("started %d jobs\n", numjobs)

	result := <-results
	for range GOROUTINES - 1 {
		r := <-results
		for i := range r {
			t := &r[i]
			if t.count > 0 {
				result.Get(t.name()).merge(t)
			}
		}
	}
	slices.SortFunc(result, func(a, b StationData) int { return bytes.Compare(a.name(), b.name()) })
	result = result[slices.IndexFunc(result, func(s StationData) bool { return s.nameidx > 0 }):]

	outf := try(os.OpenFile("results.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm))("open result file")
	outf.Seek(0, io.SeekStart)
	out := bufio.NewWriter(outf)
	for _, t := range result {
		try(fmt.Fprintf(out, "%s=%.1f/%.1f/%.1f,\n", t.name(), float64(t.min)/10, float64(t.sum)/float64(t.count)/10, float64(t.max)/10))("write result line")
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
		smap.Get(b[:i]).add(parse(b[i : i+j]))
		b = b[i+j+1:]
	}
	return first, bytes.Clone(b)
}

func parse(numb []byte) int16 {
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
