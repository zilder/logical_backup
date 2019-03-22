package decoder

// based on https://github.com/kyleconroy/pgoutput

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/mkabilov/logical_backup/pkg/message"
	"github.com/mkabilov/logical_backup/pkg/utils/dbutils"
)

type decoder struct {
	order binary.ByteOrder
	buf   *bytes.Buffer
}

const (
	truncateCascadeBit         = 1
	truncateRestartIdentityBit = 2
)

func (d *decoder) bool() bool { return d.buf.Next(1)[0] != 0 }

func (d *decoder) uint8() uint8     { return d.buf.Next(1)[0] }
func (d *decoder) uint16() uint16   { return d.order.Uint16(d.buf.Next(2)) }
func (d *decoder) uint32() uint32   { return d.order.Uint32(d.buf.Next(4)) }
func (d *decoder) uint64() uint64   { return d.order.Uint64(d.buf.Next(8)) }
func (d *decoder) oid() dbutils.OID { return dbutils.OID(d.uint32()) }
func (d *decoder) lsn() dbutils.LSN { return dbutils.LSN(d.uint64()) }

func (d *decoder) int8() int8   { return int8(d.uint8()) }
func (d *decoder) int16() int16 { return int16(d.uint16()) }
func (d *decoder) int32() int32 { return int32(d.uint32()) }
func (d *decoder) int64() int64 { return int64(d.uint64()) }

func (d *decoder) string() string {
	s, err := d.buf.ReadBytes(0)
	if err != nil {
		panic(err)
	}

	return string(s[:len(s)-1])
}

func (d *decoder) timestamp() time.Time {
	micro := int(d.uint64())
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	return ts.Add(time.Duration(micro) * time.Microsecond)
}

func (d *decoder) rowInfo(char byte) bool {
	if d.buf.Next(1)[0] == char {
		return true
	} else {
		d.buf.UnreadByte()
		return false
	}
}

func (d *decoder) tupledata() []message.TupleData {
	size := int(d.uint16())
	data := make([]message.TupleData, size)
	for i := 0; i < size; i++ {
		switch d.buf.Next(1)[0] {
		case 'n':
			data[i] = message.TupleData{Kind: message.TupleNull, Value: []byte{}}
		case 'u':
			data[i] = message.TupleData{Kind: message.TupleToasted, Value: []byte{}}
		case 't':
			vsize := int(d.order.Uint32(d.buf.Next(4)))
			data[i] = message.TupleData{Kind: message.TupleText, Value: d.buf.Next(vsize)}
		}
	}

	return data
}

func (d *decoder) columns() []message.Column {
	size := int(d.uint16())
	data := make([]message.Column, size)
	for i := 0; i < size; i++ {
		data[i] = message.Column{}
		data[i].IsKey = d.bool()
		data[i].Name = d.string()
		data[i].TypeOID = d.oid()
		data[i].Mode = d.int32()
	}

	return data
}

// Parse a logical replication message.
// See https://www.postgresql.org/docs/current/static/protocol-logicalrep-message-formats.html
func Parse(src []byte) (message.Message, error) {
	var msg message.Message

	msgType := src[0]
	d := &decoder{order: binary.BigEndian, buf: bytes.NewBuffer(src[1:])}

	switch msgType {
	case 'B':
		b := &message.Begin{}

		b.FinalLSN = d.lsn()
		b.Timestamp = d.timestamp()
		b.XID = d.int32()
		msg = b

	case 'C':
		c := &message.Commit{}

		c.Flags = d.uint8()
		c.LSN = d.lsn()
		c.TransactionLSN = d.lsn()
		c.Timestamp = d.timestamp()
		msg = c

	case 'O':
		o := &message.Origin{}
		o.LSN = d.lsn()
		o.Name = d.string()
		msg = o

	case 'R':
		r := &message.Relation{}

		r.OID = d.oid()
		r.Namespace = d.string()
		r.Name = d.string()
		r.ReplicaIdentity = message.ReplicaIdentity(d.uint8())
		r.Columns = d.columns()
		msg = r

	case 'Y':
		t := &message.Type{}

		t.OID = d.oid()
		t.Namespace = d.string()
		t.Name = d.string()
		msg = t

	case 'I':
		i := &message.Insert{}

		i.RelationOID = d.oid()
		i.IsNew = d.uint8() == 'N'
		i.NewRow = d.tupledata()
		msg = i

	case 'U':
		u := &message.Update{}

		u.RelationOID = d.oid()
		u.IsKey = d.rowInfo('K')
		u.IsOld = d.rowInfo('O')
		if u.IsKey || u.IsOld {
			u.OldRow = d.tupledata()
		}
		u.IsNew = d.uint8() == 'N'
		u.NewRow = d.tupledata()
		msg = u

	case 'D':
		m := &message.Delete{}

		m.RelationOID = d.oid()
		m.IsKey = d.rowInfo('K')
		m.IsOld = d.rowInfo('O')
		m.OldRow = d.tupledata()
		msg = m

	case 'T':
		t := &message.Truncate{}

		relationsCnt := int(d.uint32())
		options := d.uint8()
		t.Cascade = options&truncateCascadeBit == 1
		t.RestartIdentity = options&truncateRestartIdentityBit == 1

		t.RelationOIDs = make([]dbutils.OID, relationsCnt)
		for i := 0; i < relationsCnt; i++ {
			t.RelationOIDs[i] = d.oid()
		}
		msg = t

	default:
		return nil, fmt.Errorf("unknown message type for %s (%d)", []byte{msgType}, msgType)
	}

	msg.SetRawData(src);
	return msg, nil
}
