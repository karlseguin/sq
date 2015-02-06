package garbage4

import (
	"os"
	"strconv"
	"syscall"
	"unsafe"
	"time"
)

type Storage struct {
	id    uint64
	ref   []byte
	file  *os.File
	data  *[maxQueueSize]byte
}

func newStorage(t *Topic) *Storage {
	storage := openStorage(t, uint64(time.Now().UnixNano()))
	storage.file.Truncate(maxQueueSize)
	return storage
}

func openStorage(t *Topic, id uint64) *Storage {
	name := "data/" + t.name + "." + strconv.FormatUint(id, 10) + ".q"
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	ref, err := syscall.Mmap(int(file.Fd()), 0, maxQueueSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	return &Storage{
		id: id,
		ref:   ref,
		file:  file,
		data:  (*[maxQueueSize]byte)(unsafe.Pointer(&ref[0])),
	}
}
