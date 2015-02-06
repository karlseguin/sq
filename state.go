package garbage4

import (
	"os"
	"io"
)

func saveState(t *Topic) {
	name := PATH +  t.name + "/state.q"
	tmp := name + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}

	buffer := make([]byte, 8)
	writeUint64(buffer, file, uint64(t.current.id))
	writeUint64(buffer, file, uint64(t.offset))
	file.Close()

	if err := os.Rename(tmp, name); err != nil {
		panic(err)
	}
}

func writeUint64(buffer []byte, writer io.Writer, value uint64) {
	encoder.PutUint64(buffer, value)
	writer.Write(buffer)
}

func loadState(t *Topic) bool {
	root := PATH +  t.name
	os.MkdirAll(root, 0700)
	file, err := os.Open(root + "/state.q")
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		panic(err)
	}
	defer file.Close()
	buffer := make([]byte, 8)
	storage := openStorage(t, readUint64(buffer, file))
	offset := readUint64(buffer, file)
	t.current = storage

	end := uint64(MAX_QUEUE_SIZE - 4)
	for offset < end {
		l := encoder.Uint32(storage.data[offset:])
		if l == 0 {
			break
		}
		offset += uint64(l) + 4
	}

	t.offset = int(offset)
	return true
}

func readUint64(buffer []byte, reader io.Reader) uint64 {
	reader.Read(buffer)
	return encoder.Uint64(buffer)
}
