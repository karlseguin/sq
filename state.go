package garbage4

import (
	"os"
	"syscall"
	"unsafe"
)

const MAX_STATE_SIZE = 32768

type State struct {
	ref      []byte
	file     *os.File
	offset   int
	data     *[MAX_STATE_SIZE]byte
	channels map[string]int
}

type Position struct {
	offset    int
	segmentId uint64
}

func loadState(t *Topic) (*State, error) {
	path := PATH + t.name + "/state.q"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if size := info.Size(); size == 0 {
		file.Truncate(MAX_STATE_SIZE)
	} else if size != MAX_STATE_SIZE {
		panic("invalid state file size")
	}

	ref, err := syscall.Mmap(int(file.Fd()), 0, MAX_STATE_SIZE, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}

	state := &State{
		ref:      ref,
		file:     file,
		channels: make(map[string]int),
		data:     (*[MAX_STATE_SIZE]byte)(unsafe.Pointer(&ref[0])),
	}

	start, end := 16, 16
	for {
		if state.data[end] == 0 {
			if end == start {
				break
			}
			offset := end + 1
			state.channels[string(state.data[start:end])] = offset
			start = offset + 16
			end = start
		} else {
			end++
		}
	}
	state.offset = end
	return state, nil
}

func (s *State) loadPosition(offset int) *Position {
	return (*Position)(unsafe.Pointer(&s.data[offset]))
}

func (s *State) loadOrCreatePosition(name string) *Position {
	offset, exists := s.channels[name]
	if exists == false {
		//todo check for overflow
		//todo compact
		s.offset += copy(s.data[s.offset:], name)
		s.data[s.offset] = 0
		offset = s.offset + 1
		s.offset += 17
	}
	return s.loadPosition(offset)
}

func (s *State) Close() {
	syscall.Munmap(s.ref)
	s.file.Close()
	s.data, s.ref = nil, nil
}

// func saveState(t *Topic, full bool) {
// 	name := PATH + t.name + "/state.q"
// 	tmp := name + ".tmp"
// 	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
// 	if err != nil {
// 		panic(err)
// 	}

// 	buffer := make([]byte, 8)
// 	writeUint64(buffer, file, uint64(t.segment.id))
// 	writeUint64(buffer, file, uint64(t.offset))
// 	if full {
// 		// for
// 	}
// 	file.Close()
// 	if err := os.Rename(tmp, name); err != nil {
// 		panic(err)
// 	}
// }

// func writeUint64(buffer []byte, writer io.Writer, value uint64) {
// 	encoder.PutUint64(buffer, value)
// 	writer.Write(buffer)
// }

// func loadState(t *Topic) bool {
// 	root := PATH + t.name
// 	os.MkdirAll(root, 0700)
// 	file, err := os.Open(root + "/state.q")
// 	if os.IsNotExist(err) {
// 		return false
// 	}
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()
// 	buffer := make([]byte, 8)
// 	t.segment = openSegment(t, readUint64(buffer, file))
// 	offset := readUint64(buffer, file)

// 	end := uint64(MAX_QUEUE_SIZE - 4)
// 	for offset < end {
// 		l := encoder.Uint32(t.segment.data[offset:])
// 		if l == 0 {
// 			break
// 		}
// 		offset += uint64(l) + 4
// 	}

// 	t.offset = int(offset)
// 	return true
// }

// func readUint64(buffer []byte, reader io.Reader) uint64 {
// 	reader.Read(buffer)
// 	return encoder.Uint64(buffer)
// }
