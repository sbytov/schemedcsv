package schemedcsv

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEncoder_EncodeNumeric(t *testing.T) {
	a := struct {
		Int8   int8   `csv:"int8"`
		Int16  int16  `csv:"int16"`
		Int32  int32  `csv:"int32"`
		Int64  int64  `csv:"int64"`
		Int    int    `csv:"int"`
		Uint8  uint8  `csv:"uint8"`
		Uint16 uint16 `csv:"uint16"`
		Uint32 uint32 `csv:"uint32"`
		Uint64 uint64 `csv:"uint64"`
		Uint   uint   `csv:"uint"`
	}{
		-1, -2, -3, -4, -5, 6, 7, 8, 9, 10,
	}
	e, err := NewEncoderWithOpts(a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(a)
	require.NoError(t, err)
	csvBytes := e.Bytes()
	// TODO: 32 bit support?
	require.Equal(t, "#schema: int8 tinyint,int16 smallint,int32 int,int64 bigint,int bigint,uint8 tinyint,uint16 smallint,uint32 int,uint64 bigint,uint bigint", string(e.Schema()))
	require.Equal(t, "-1,-2,-3,-4,-5,6,7,8,9,10", string(csvBytes))
	require.Equal(t, 10, len(bytes.Split(csvBytes, []byte{','})))
}

func TestEncoder_EncodeUnsigned(t *testing.T) {
	a := struct {
		Uint8  uint8  `csv:"uint8"`
		Uint16 uint16 `csv:"uint16"`
		Uint32 uint32 `csv:"uint32"`
		Uint64 uint64 `csv:"uint64"`
	}{
		0xFF, 0xFFFF, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF,
	}
	e, err := NewEncoderWithOpts(a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(a)
	require.NoError(t, err)
	csvBytes := e.Bytes()
	// TODO: 32 bit support?
	require.Equal(t, "#schema: uint8 tinyint,uint16 smallint,uint32 int,uint64 bigint", string(e.Schema()))
	require.Equal(t, "-127,-32767,-2147483647,-9223372036854775807", string(csvBytes))
	require.Equal(t, 4, len(bytes.Split(csvBytes, []byte{','})))
}

func TestEncoder_EncodeDouble(t *testing.T) {
	a := struct {
		Float32 float32 `csv:"float32"`
		Float64 float64 `csv:"float64"`
	}{
		0.42, 42e5,
	}
	e, err := NewEncoderWithOpts(a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(a)
	require.NoError(t, err)
	csvBytes := e.Bytes()
	require.Equal(t, "#schema: float32 float,float64 double", string(e.Schema()))
	require.Equal(t, "0.42,4.2e+06", string(csvBytes))
	require.Equal(t, 2, len(bytes.Split(csvBytes, []byte{','})))
}

func TestEncoder_EncodeTime(t *testing.T) {
	ti := time.Date(2020, 10, 5, 13, 14, 15, 16000000, time.UTC)
	a := struct {
		Local time.Time     `csv:"time_local"`
		UTC   time.Time     `csv:"time_utc"`
		Nil   time.Time     `csv:"time_nil"`
		Dur   time.Duration `csv:"duration"`
	}{
		ti.Local(), ti.UTC(), time.Time{}, time.Millisecond * 155,
	}
	e, err := NewEncoderWithOpts(a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(a)
	require.NoError(t, err)
	csvBytes := e.Bytes()
	require.Equal(t, "#schema: time_local timestamp,time_utc timestamp,time_nil timestamp,duration bigint", string(e.Schema()))
	require.Equal(t, "2020-10-05T13:14:15.016Z,2020-10-05T13:14:15.016Z,,155000000", string(csvBytes))
	require.Equal(t, 4, len(bytes.Split(csvBytes, []byte{','})))
}

func TestEncoder_EncodeBoolean(t *testing.T) {
	a := struct {
		True  bool `csv:"true"`
		False bool `csv:"false"`
	}{
		true, false,
	}
	e, err := NewEncoderWithOpts(a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(a)
	require.NoError(t, err)
	csvBytes := e.Bytes()
	require.Equal(t, "#schema: true tinyint,false tinyint", string(e.Schema()))
	require.Equal(t, "1,0", string(csvBytes))
	require.Equal(t, 2, len(bytes.Split(csvBytes, []byte{','})))
}

func TestEncoder_EncodePointer(t *testing.T) {
	var i uint64 = 42
	var b = true
	var s = "hello"
	type st struct {
		Int int32  `csv:"int"`
		Str string `csv:"string"`
	}
	a := struct {
		PtrToInt    *uint64 `csv:"ptrtoint"`
		PtrToBool   *bool   `csv:"ptrtobool"`
		PtrToString *string `csv:"ptrtostring"`
		PtrToStruct *st     `csv:"ptrtostruct"`
	}{
		&i, &b, &s, &st{13, "thirteen"},
	}
	e, err := NewEncoderWithOpts(a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(a)
	require.NoError(t, err)
	csvBytes := e.Bytes()
	require.Equal(t, "#schema: ptrtoint bigint,ptrtobool tinyint,ptrtostring string,int int,string string", string(e.Schema()))
	require.Equal(t, "42,1,hello,13,thirteen", string(csvBytes))
	require.Equal(t, 5, len(bytes.Split(csvBytes, []byte{','})))
}

func TestEncoder_EncodeStrings(t *testing.T) {
	a := struct {
		NoComma string `csv:"empty"`
		Comma   string `csv:"comma"`
		NewLine string `csv:"newline"`
		Quotes  string `csv:"quotes"`
	}{
		"", "hello, world", "new\nline", "\"quoted\"",
	}
	b := struct {
		Cyrillic string `csv:"cyrillic"`
		Korean   string `csv:"korean"`
	}{
		"прёвёт", "星期三",
	}
	e, err := NewEncoderWithOpts(a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(a)
	require.NoError(t, err)
	csvBytes := e.Bytes()
	require.Equal(t, "#schema: empty string,comma string,newline string,quotes string", string(e.Schema()))
	require.Equal(t, `,"hello, world",new\nline,"""quoted"""`, string(csvBytes))

	e, err = NewEncoderWithOpts(b, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	err = e.Encode(b)
	require.NoError(t, err)
	csvBytes = e.Bytes()
	require.Equal(t, "#schema: cyrillic string,korean string", string(e.Schema()))
	require.Equal(t, []byte{0xD0, 0xBF, 0xD1, 0x80, 0xD1, 0x91, 0xD0, 0xB2, 0xD1, 0x91, 0xD1, 0x82, ',', 0xE6, 0x98, 0x9F, 0xE6, 0x9C, 0x9F, 0xE4, 0xB8, 0x89}, csvBytes)
}

func TestEncoder_EncodeSlice(t *testing.T) {
	objects := []struct {
		Strings []string `csv:"strings"`
	}{
		{nil},
		{[]string{"", "hello, world", "прёвёт", "星期三"}},
		{[]string{"simple", "text"}},
	}
	for _, obj := range objects {
		e, err := NewEncoderWithOpts(obj, EncoderOpts{NoKey: true})
		require.NoError(t, err)
		err = e.Encode(obj)
		require.NoError(t, err)
		csvBytes := e.Bytes()
		require.Equal(t, "#schema: strings string", string(e.Schema()))
		csvReader := csv.NewReader(bytes.NewReader(csvBytes))
		csvLines, err := csvReader.ReadAll()
		require.NoError(t, err)
		require.Len(t, csvLines, 1)
		require.Len(t, csvLines[0], 1)
		var decodedVal []string
		require.NoError(t, json.Unmarshal([]byte(csvLines[0][0]), &decodedVal))
		wantVal := obj.Strings
		if len(wantVal) == 0 {
			wantVal = []string{}
		}
		require.Equal(t, wantVal, decodedVal)
	}
}

func TestEncoder_EncodeOrder(t *testing.T) {
	a := struct {
		Key    string `csv:"key, key"`
		Field1 string `csv:"field1, order=3"`
		Field2 string `csv:"field2, order=2"`
		Field3 string `csv:"field3, order=1"`
	}{}
	e, err := NewEncoder(a)
	require.NoError(t, err)
	require.Equal(t, "#order: field3,field2,field1", string(e.Order()))

	b := struct {
		Field1 string `csv:"field1"`
		Field2 string `csv:"field2"`
		Field3 string `csv:"field3"`
	}{}
	e, err = NewEncoderWithOpts(b, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	require.Equal(t, "#order: ", string(e.Order()))

	c := struct {
		Key    string `csv:"key, key"`
		Field1 string `csv:"field1, order=3"`
		Field2 string `csv:"field2, order=-2"`
		Field3 string `csv:"field3, order=1"`
	}{}
	e, err = NewEncoder(c)
	require.NoError(t, err)
	require.Equal(t, "#order: field2,field3,field1", string(e.Order()))
}

func TestEncoder_EncodePointerToStruct(t *testing.T) {
	a := struct {
		Field1 string `csv:"field1"`
		Field2 string `csv:"field2"`
		Field3 string `csv:"field3"`
	}{
		Field1: "1",
		Field2: "2",
		Field3: "3",
	}
	e, err := NewEncoderWithOpts(&a, EncoderOpts{NoKey: true})
	require.NoError(t, err)
	require.Equal(t, "#schema: field1 string,field2 string,field3 string", string(e.Schema()))
	require.NoError(t, e.Encode(&a))
	require.NoError(t, err)
	require.Equal(t, "1,2,3", e.String())
}

func TestEncoder_EncodeMultipleLines(t *testing.T) {
	a := []struct {
		Field1 string `csv:"field1"`
		Field2 string `csv:"field2"`
		Field3 string `csv:"field3"`
	}{
		{
			Field1: "1",
			Field2: "2",
			Field3: "3",
		},
		{
			Field1: "4",
			Field2: "5",
			Field3: "6",
		},
	}
	e, err := NewEncoderWithOpts(&a[0], EncoderOpts{NoKey: true})
	require.NoError(t, err)
	require.Equal(t, "#schema: field1 string,field2 string,field3 string", string(e.Schema()))
	for _, i := range a {
		require.NoError(t, e.EncodeLine(&i)) //nolint
	}
	require.NoError(t, err)
	require.Equal(t, `1,2,3
4,5,6
`, e.String())

	e.Reset()
	for _, i := range a {
		require.NoError(t, e.Encode(&i)) //nolint
		e.WriteByte('|')
	}
	require.NoError(t, err)
	require.Equal(t, `1,2,3|4,5,6|`, e.String())
}

func TestEncoder_EncodeMeta(t *testing.T) {
	type a struct {
		Field1 string `csv:"field1, key"`
		Field2 string `csv:"field2"`
		Field3 string `csv:"field3"`
	}
	e, err := NewEncoder(&a{})
	require.NoError(t, err)
	require.Equal(t, `#schema: field1 string,field2 string,field3 string
#key: field1
#order: 
`, string(e.MetaBytes(AllMeta)))

	type b struct {
		Field1 string `csv:"field1, key"`
		Field2 string `csv:"field2"`
	}
	e, err = NewEncoder(&b{})
	require.NoError(t, err)
	require.Equal(t, `#schema: field1 string,field2 string
#key: field1
#order: 
`, string(e.MetaBytes(AllMeta)))
}

func TestEncoder_CompositeKey(t *testing.T) {
	type a struct {
		Field1 string `csv:"field1, key"`
		Field2 string `csv:"field2, key"`
	}
	e, err := NewEncoder(&a{})
	require.NoError(t, err)
	require.Equal(t, `#schema: field1 string,field2 string
#key: field1,field2
#order: 
`, string(e.MetaBytes(AllMeta)))
}
