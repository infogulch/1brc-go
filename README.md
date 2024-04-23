# 1brc-go

This is my version of the [1 Billion Row Challenge](1brc) (1brc), written in Go.
Inspired by blog posts by [Renato Pereira](r2p) and [Shraddha Agrawal](bsg).

## Setup and usage

```shell
# build and run once to generate profile in default.pgo
go build
./1brc-go -profile

# build and run again; go automatically uses 'default.pgo` file for profile-guided optimization
go build
time ./1brc-go

# view profile (requires graphviz)
go tool pprof -http 127.0.0.1:8080 default.pgo
```

[1brc]: https://github.com/gunnarmorling/1brc
[comments]: https://news.ycombinator.com/item?id=38851337
[r2p]: https://r2p.dev/b/2024-03-18-1brc-go/#:~:text=One%20Billion%20Row%20Challenge%20in%20Golang%20%2D%20From%2095s%20to%201.96s
[bsg]: https://www.bytesizego.com/blog/one-billion-row-challenge-go
