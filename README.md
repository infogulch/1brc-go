# 1brc-go

This is a version of the [1 Billion Row Challenge][1brc] (1brc) written in Go.
Inspired by blog posts by [Renato Pereira (r2p.dev)][r2p] and
[Shraddha Agrawal (bytesizego.com)][bsg].

[1brc]: https://github.com/gunnarmorling/1brc
[r2p]: https://r2p.dev/b/2024-03-18-1brc-go/
[bsg]: https://www.bytesizego.com/blog/one-billion-row-challenge-go

It is written from scratch with some ideas similar to the r2p.dev post, with the
following differences:

* Each worker manages its own buffer and summary data map, reading data from the
  file into its buffer from a starting offset using the `io.ReaderAt` interface.
  This simplifies reading, balances IO bandwidth with worker speed, and
  avoids memory access patterns that could cause cache coherency slowdowns.
  * Currently it runs `NumCPUs*2` workers, the idea being that one worker can be
    busy processing records while another is waiting for the OS to read data
    from the file. Anything past 8 has diminishing returns for my PC which has a
    measly 4 cores from 2016, ymmv.
* Since the start and end offsets could be the middle of a line, workers handle
  the first and last lines specially. Each job is given two channels to
  communicate these lines with adjacent jobs. When a worker is done processing a
  buffer it first sends its last line to the next job, then it receives the last
  line from the previous job and combines that with the first line from its
  current job to process it. These cross-job syncs are infrequent enough that
  their impact is negligible.
* The buffer is scanned using the stdlib `bytes.IndexByte` to find record
  separators. This func is hand written in assembly to find bytes quickly.
* The summary struct StationData uses the smallest data types required to
  represent the data, and is only 16 bytes.
* This repo contains the sample test data from the original 1brc repo under
  `samples/` and includes Go tests showing that this implementation passes all
  tests.
* It uses Go's built-in map instead of a swisstable (this may change), and
  safely uses `unsafe.String` to temporarily transmute a `[]byte` to `string`
  during map lookup.
* The implementation is 25% smaller in LOC, and (on my machine) about 20% faster
  than the r2p.dev implementation.

## Setup and usage

```shell
# build and run once or twice to generate a suitable profile in default.pgo
go run -profile

# build and run again
# Note, Go automatically uses 'default.pgo` file for profile-guided optimization,
# but it doesn't make a huge difference in my experience.
go build
time ./1brc-go

# view profile (requires graphviz)
go tool pprof -http 127.0.0.1:8080 default.pgo
```

Generate your own data:

```shell
# ensure you have java version 21:
java --version

# clone the original 1brc repo:
git clone https://github.com/gunnarmorling/1brc
cd 1brc

# generate 1B measurements. Resulting file is ~13GB
./create_measurements.sh 1000000000

# See alternative generators: ./create_measurements*.sh
```
