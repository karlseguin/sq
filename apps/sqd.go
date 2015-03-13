package main

import (
	"flag"
	"github.com/karlseguin/sq"
	"github.com/karlseguin/sq/sqd"
)

var (
	address = flag.String("address", ":6543", "TCP address to listen on")
	data    = flag.String("data", "/tmp/sq/", "path to store data files")
	ss      = flag.Uint("ss", 16777216, "size of each segment, in bytes")
	bs      = flag.Uint("bs", 16384, "size of a typical message")
)

func main() {
	flag.Parse()
	config := &sqd.Configuration{
		Address:    *address,
		Topics:     sq.ConfigureTopic().Path(*data).SegmentSize(uint32(*ss)),
		BufferSize: int(*bs),
	}
	sqd.Listen(config)
}
