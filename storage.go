package garbage4

import (
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

type Storage struct {
	ref   []byte
	index int
	file  *os.File
	data  *[maxQueueSize]byte
}

func newStorage(name string, index int) *Storage {
	name = name + "." + strconv.Itoa(index) + ".q"
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	file.Truncate(maxQueueSize)
	ref, err := syscall.Mmap(int(file.Fd()), 0, maxQueueSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	return &Storage{
		ref:   ref,
		file:  file,
		index: index,
		data:  (*[maxQueueSize]byte)(unsafe.Pointer(&ref[0])),
	}
}
