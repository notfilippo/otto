package traces

import (
	"encoding/binary"
	"io"
	"time"
)

type Op uint8

const (
	OpSet Op = iota
	OpGet
)

type Entry struct {
	Timestamp time.Time
	Id        uint64
	Size      uint64
	Op        Op
}

type Encoder struct {
	w io.Writer
}

// NewEncoder creates a new trace writer.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w}
}

// Encode writes a new entry to the trace stream.
func (e *Encoder) Encode(entry Entry) error {
	data := []any{
		entry.Timestamp.UnixMicro(),
		entry.Id,
		entry.Size,
		entry.Op,
	}

	for _, v := range data {
		if err := binary.Write(e.w, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	return nil
}

type Decoder struct {
	r io.Reader
}

// NewDecoder creates a new trace reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r}
}

func (d *Decoder) Decode() (Entry, error) {
	var entry Entry

	var unixMicro int64
	if err := binary.Read(d.r, binary.LittleEndian, &unixMicro); err != nil {
		return entry, err
	}
	entry.Timestamp = time.UnixMicro(unixMicro)

	data := []any{
		&entry.Id,
		&entry.Size,
		&entry.Op,
	}

	for _, v := range data {
		if err := binary.Read(d.r, binary.LittleEndian, v); err != nil {
			return entry, err
		}

	}

	return entry, nil
}

// Iter implements iter.Seq[Entry].
func (d *Decoder) Iter(yield func(entry Entry) bool) {
	for {
		entry, err := d.Decode()
		if err == io.EOF {
			return
		}

		if !yield(entry) {
			return
		}
	}
}
