package schemedcsv

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"git.acronis.com/abc/go-libs/log"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unsafe"
)

const (
	csvTag     = "csv"
	tagKey     = "key"
	tagOrder   = "order"

	String    = "string"
	Bigint    = "bigint"
	Int       = "int"
	Smallint  = "smallint"
	Tinyint   = "tinyint"
	Timestamp = "timestamp"
	Double    = "double"
	Float     = "float"
	Date      = "date"

	SchemaHeader  = "#schema:"
	KeyHeader     = "#key:"
	OrderHeader   = "#order:"

	CSVSeparator = ','

	timePkg  = "time"
	timeType = "Time"

	RFC3339Milli = "2006-01-02T15:04:05.000Z"

	NilTenant = "-1"
)

type Column struct {
	Name    string
	Type    string
	IsKey   bool
	Order   *int
}

type Metadata struct {
	Columns       []Column
	KeyIdx        []int
	OrderIdx      []int
}

// Encoder encodes go structs as schemaful CSV
type Encoder struct {
	se  structEncoder
	te  timeEncoder
	enc map[reflect.Kind]scalarEncoder
	csv *csv.Writer
	typ reflect.Type

	*bytes.Buffer
	Meta Metadata
	opts EncoderOpts
}

type structEncoder struct {
	fields []field
	parent *Encoder
}

type EncoderOpts struct {
	NoKey bool
}

func NewEncoderLogWarnings(v interface{}, opts EncoderOpts, logger log.FieldLogger) (*Encoder, error) {
	buf := &bytes.Buffer{}
	buf.Grow(1024)
	e := &Encoder{
		enc:    make(map[reflect.Kind]scalarEncoder),
		Buffer: buf,
		csv:    csv.NewWriter(buf),
		opts:   opts,
	}
	e.se.parent = e // encoders hierarchy will have common parent *Encoder to access its buf, csv, and scalar encoders cache
	var err error
	e.typ, _, err = e.validateType(v)
	if err != nil {
		return nil, err
	}
	err = e.se.populate(e.typ)
	if err != nil {
		return nil, err
	}

	e.se.annotations(&e.Meta.Columns)
	err = e.Meta.validate(e.opts.NoKey, logger)
	if err != nil {
		return nil, fmt.Errorf("invalid annotations: %w", err)
	}
	return e, nil
}

// NewEncoder returns a new encoder instance for struct v.
// v is used to determine csv schema via reflection.
// v can be either struct or pointer to struct.
// See https://adn.acronis.com/display/csv/csv+service+API+and+data+structure for details.
// v fields that must be exported to csv need to be public and annotated with 'csv' annotations, like this:
// type S struct {
// Field string 'csv:"csv_field[, key][,order=N][,deident][,tenant]"'
// }
// where csv_field is the name of column in csv CSV;
//		 key marks the column as part of primary key, in case of composite PK can be multiple columns;
//		 deident marks the column as requiring deidentification;
//		 tenant marks the columns as containing tenant ID;
//		 order marks the columns for sorting (see spec) and defines the sort order according to N ascending.
func NewEncoder(v interface{}) (*Encoder, error) {
	return NewEncoderLogWarnings(v, EncoderOpts{}, log.NewDisabledLogger())
}

// NewEncoderWithOpts is like NewEncoder but allows to provide encoding options.
// Currently the only option is NoKey which indicates that the entity is immutable and all its
// columns are forming a composite #key implicitly.
func NewEncoderWithOpts(v interface{}, opts EncoderOpts) (*Encoder, error) {
	return NewEncoderLogWarnings(v, opts, log.NewDisabledLogger())
}

// EncodeStrings encodes given strings as csv fields of type string.
func (e *Encoder) EncodeStrings(s []string) error {
	if len(s) == 0 {
		return nil
	}
	e.Buffer.WriteRune(CSVSeparator)
	return e.encodeStrings(s)
}

// Encode encodes struct v as csv fields into embedded bytes.Buffer.
// v can be either a struct or a pointer to struct.
// The struct type must correspond to the type of struct passed to NewEncoder.
func (e *Encoder) Encode(v interface{}) error {
	typ, isPtr, err := e.validateType(v)
	if err != nil {
		return err
	}
	val := reflect.ValueOf(v)
	if isPtr {
		val = val.Elem()
	}
	base := reflect.New(typ)
	base.Elem().Set(val)
	err = e.se.encode(unsafe.Pointer(base.Pointer()), e.Buffer)
	return err
}

// EncodeLine is a wrapper around Encode that adds newline in the end.
func (e *Encoder) EncodeLine(v interface{}) error {
	err := e.Encode(v)
	if err != nil {
		return nil
	}
	return e.Buffer.WriteByte('\n')
}

//nolint: gocyclo
func (m *Metadata) build(noKey bool) error {
	anns := m.Columns
	for i, a := range anns {
		if a.IsKey {
			m.KeyIdx = append(m.KeyIdx, i)
		}
	}
	m.OrderIdx = buildOrder(anns)
	if noKey && len(m.KeyIdx) == 0 {
		for i := range m.Columns {
			m.KeyIdx = append(m.KeyIdx, i)
		}
	}
	return nil
}

func buildOrder(anns []Column) []int {
	type orderItem struct {
		order int
		idx   int
	}
	var orderItems []orderItem
	for i, a := range anns {
		if a.Order != nil {
			orderItems = append(orderItems, orderItem{order: *a.Order, idx: i})
		}
	}
	sort.Slice(orderItems, func(i, j int) bool {
		return orderItems[i].order < orderItems[j].order
	})
	var idx []int
	for _, oi := range orderItems {
		idx = append(idx, oi.idx)
	}
	return idx
}

type MetaKind int

const (
	Schema MetaKind = 1 << iota
	Keys
	Order
	IDs
	Tenant
)

const AllMeta = Schema | Keys | Order | IDs | Tenant

// MetaBytes returns selected csv Meta.
func (e *Encoder) MetaBytes(kind MetaKind) []byte {
	var buf bytes.Buffer
	if kind&Schema != 0 {
		buf.Write(e.Schema())
		buf.WriteByte('\n')
	}
	if kind&Keys != 0 {
		buf.Write(e.Keys())
		buf.WriteByte('\n')
	}
	if kind&Order != 0 {
		buf.Write(e.Order())
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// Schema returns csv #schema Meta.
func (e *Encoder) Schema() []byte {
	idx := make([]int, 0, len(e.Meta.Columns))
	for i := range e.Meta.Columns {
		idx = append(idx, i)
	}
	return e.Meta.printColumns(SchemaHeader, idx, func(col Column) string {
		return fmt.Sprintf("%v %v", col.Name, col.Type)
	})
}

// Keys returns csv #keys Meta.
func (e *Encoder) Keys() []byte {
	return e.Meta.printColumns(KeyHeader, e.Meta.KeyIdx, func(col Column) string {
		return col.Name
	})
}

// Order return csv #order Meta.
func (e *Encoder) Order() []byte {
	return e.Meta.printColumns(OrderHeader, e.Meta.OrderIdx, func(col Column) string {
		return col.Name
	})
}

func (e *Encoder) validateType(v interface{}) (reflect.Type, bool, error) {
	if v == nil {
		return nil, false, fmt.Errorf("nil interface passed in")
	}
	typ := reflect.TypeOf(v)
	isPtr := false
	if typ.Kind() == reflect.Ptr {
		isPtr = true
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, false, fmt.Errorf("only struct or *struct types supported")
	}
	if e.typ != nil && typ != e.typ {
		return nil, false, fmt.Errorf("invalid type passed in, %v expected", e.typ)
	}
	return typ, isPtr, nil
}

type field struct {
	offset  uintptr
	ann     Column
	size    uintptr
	kind    reflect.Kind
	encoder encoder
}

type timeEncoder struct {
}

type encoder interface {
	encode(bp unsafe.Pointer, buf *bytes.Buffer) error
}

//nolint: interfacer
func (e *timeEncoder) encode(bp unsafe.Pointer, buf *bytes.Buffer) error {
	t := *(*time.Time)(bp)
	if t.IsZero() {
		return nil
	}
	str := t.UTC().Format(RFC3339Milli)
	buf.WriteString(str)
	return nil
}

type scalarEncoder struct {
	kind   reflect.Kind
	parent *Encoder
}

//nolint:gocyclo
func (e *scalarEncoder) encode(bp unsafe.Pointer, buf *bytes.Buffer) error {
	switch e.kind {
	case reflect.Int8:
		writeInt(int64(*(*int8)(bp)), buf)
	case reflect.Int16:
		writeInt(int64(*(*int16)(bp)), buf)
	case reflect.Int32:
		writeInt(int64(*(*int32)(bp)), buf)
	case reflect.Int64:
		writeInt(*(*int64)(bp), buf)
	case reflect.Int:
		writeInt(int64(*(*int)(bp)), buf)
	case reflect.Uint8:
		writeUint(uint64(*(*uint8)(bp)), 8, buf)
	case reflect.Uint16:
		writeUint(uint64(*(*uint16)(bp)), 16, buf)
	case reflect.Uint32:
		writeUint(uint64(*(*uint32)(bp)), 32, buf)
	case reflect.Uint64:
		writeUint(*(*uint64)(bp), 64, buf)
	case reflect.Uint:
		writeUint(uint64(*(*uint)(bp)), strconv.IntSize, buf)
	case reflect.Bool:
		writeBool(*(*bool)(bp), buf)
	case reflect.Float32:
		buf.WriteString(fmt.Sprintf("%v", *(*float32)(bp)))
	case reflect.Float64:
		buf.WriteString(fmt.Sprintf("%v", *(*float64)(bp)))
	case reflect.String:
		str := *(*string)(bp)
		err := e.parent.encodeString(str)
		if err != nil {
			return fmt.Errorf("failed to encode string: %w", err)
		}
	default:
		return fmt.Errorf("unsupported type %q", e.kind)
	}
	return nil
}

func (e *Encoder) encodeString(s string) error {
	err := e.encodeStrings([]string{s})
	if err != nil {
		return err
	}
	e.Buffer.Truncate(e.Buffer.Len() - 1)
	return nil
}

func (e *Encoder) encodeStrings(s []string) error {
	for i := range s {
		s[i] = strings.ReplaceAll(s[i], "\n", `\n`)
	}
	err := e.csv.Write(s)
	if err != nil {
		return err
	}
	e.csv.Flush()
	return nil
}

//nolint: gocyclo
func (e *structEncoder) populate(typ reflect.Type) error {
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		if sf.PkgPath != "" { // don't handle private fields
			continue
		}
		annotation := getAnnotation(sf)
		if annotation == nil {
			continue
		}
		switch sf.Type.Kind() {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		case reflect.Bool:
		case reflect.Struct:
		case reflect.Float32, reflect.Float64:
		case reflect.String, reflect.Slice:
		case reflect.Ptr:
		default:
			return fmt.Errorf("unsupported type %q of field %q", sf.Type.Kind(), sf.Name)
		}
		if sf.Type.Kind() == reflect.Slice &&
			sf.Type.Elem().Kind() != reflect.String {
			return fmt.Errorf("unsupported slice type %q of field %q, only []string supported", sf.Type.Kind(), sf.Name)
		}
		var sube encoder
		switch sf.Type.Kind() {
		case reflect.Struct:
			if isTime(sf.Type) {
				sube = &e.parent.te
			} else {
				stre := &structEncoder{parent: e.parent}
				err := stre.populate(sf.Type)
				if err != nil {
					return err
				}
				sube = stre
			}

		case reflect.Ptr:
			switch sf.Type.Elem().Kind() {
			case reflect.Struct:
				if isTime(sf.Type.Elem()) {
					sube = &e.parent.te
				} else {
					ptre := &structEncoder{parent: e.parent}
					err := ptre.populate(sf.Type.Elem())
					if err != nil {
						return err
					}
					sube = ptre
				}
			default:
				sube = e.getScalarEncoder(sf.Type.Elem().Kind())
			}

		default:
			sube = e.getScalarEncoder(sf.Type.Kind())
		}
		e.fields = append(e.fields, field{
			sf.Offset,
			*annotation,
			sf.Type.Size(),
			sf.Type.Kind(),
			sube})
	}
	return nil
}

func (e *structEncoder) getScalarEncoder(kind reflect.Kind) encoder {
	scalarEnc, ok := e.parent.enc[kind]
	if !ok {
		scalarEnc = scalarEncoder{kind: kind, parent: e.parent}
		e.parent.enc[kind] = scalarEnc
	}
	return &scalarEnc
}

var orderRegex = regexp.MustCompile(`\s*` + tagOrder + `\s*=\s*(\-?\d+)`)
var headerColumnRegex = regexp.MustCompile(`^\s*([^()=\s]+)\s*(?:\(\s*([^()\s]+)\s*\)\s*)?$`)

func getAnnotation(sf reflect.StructField) *Column { //nolint:gocritic
	tag := sf.Tag.Get(csvTag)
	if tag == "" {
		return nil
	}
	parts := strings.Split(tag, ",")
	var (
		isKey   bool
	)
	var order *int
	for _, p := range parts[1:] {
		s := headerColumnRegex.FindStringSubmatch(p)
		if s != nil {
			tag := s[1]
			switch tag {
			case tagKey:
				isKey = true
			default:
				continue
			}
		} else {
			s = orderRegex.FindStringSubmatch(p)
			if len(s) > 1 {
				o, _ := strconv.Atoi(s[1])
				order = &o
			}
		}
	}
	return &Column{
		Name:    parts[0],
		Type:    getTypename(sf.Type),
		IsKey:   isKey,
		Order:   order,
	}
}

func getTypename(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Int8, reflect.Uint8:
		return Tinyint
	case reflect.Int16, reflect.Uint16:
		return Smallint
	case reflect.Int32, reflect.Uint32:
		return Int
	case reflect.Int, reflect.Uint:
		if strconv.IntSize == 32 {
			return Int
		}
		return Bigint
	case reflect.Uint64, reflect.Int64:
		return Bigint
	case reflect.Bool:
		return Tinyint
	case reflect.Struct:
		if isTime(t) {
			return Timestamp
		}
		return t.Name()
	case reflect.Float32:
		return Float
	case reflect.Float64:
		return Double
	case reflect.String, reflect.Slice:
		return String
	case reflect.Ptr:
		return getTypename(t.Elem())
	default:
		return t.Name()
	}
}

func isTime(t reflect.Type) bool {
	return t.PkgPath() == timePkg && t.Name() == timeType
}

//nolint: interfacer
func writeInt(v int64, buffer *bytes.Buffer) {
	var dst []byte
	buffer.Write(strconv.AppendInt(dst, v, 10))
}

//nolint: interfacer
func writeUint(v uint64, width int, buffer *bytes.Buffer) {
	var dst []byte
	i, err := UnsignedToSigned(v, width)
	if err != nil {
		return
	}
	buffer.Write(strconv.AppendInt(dst, i, 10))
}

//nolint: interfacer
func writeBool(v bool, buffer *bytes.Buffer) {
	if v {
		buffer.WriteByte('1')
	} else {
		buffer.WriteByte('0')
	}
}

func (e *structEncoder) encode(bp unsafe.Pointer, buf *bytes.Buffer) error {
	for i, f := range e.fields {
		if i > 0 {
			buf.WriteByte(CSVSeparator)
		}
		switch f.kind {
		case reflect.Slice:
			slice := *(*[]string)(unsafe.Pointer(uintptr(bp) + f.offset))
			sliceJSONStr := "[]"
			if len(slice) != 0 {
				sliceJSON, err := json.Marshal(slice)
				if err != nil {
					return fmt.Errorf("failed to marshal slice: %w", err)
				}
				sliceJSONStr = string(sliceJSON)
			}

			err := e.parent.encodeString(sliceJSONStr)
			if err != nil {
				return fmt.Errorf("failed to encode string: %w", err)
			}
		case reflect.Ptr:
			pointer := *(*unsafe.Pointer)(unsafe.Pointer(uintptr(bp) + f.offset))
			if pointer != nil {
				err := f.encoder.encode(pointer, buf)
				if err != nil {
					return err
				}
			}
		default:
			if f.encoder == nil {
				return fmt.Errorf("unsupported type %v", f.kind)
			}
			err := f.encoder.encode(unsafe.Pointer(uintptr(bp)+f.offset), buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *structEncoder) annotations(cols *[]Column) {
	for _, f := range e.fields {
		if e, ok := f.encoder.(*structEncoder); ok {
			e.annotations(cols)
		} else {
			*cols = append(*cols, f.ann)
		}
	}
}

func UnsignedToSigned(u uint64, width int) (int64, error) {
	if !(width == 64 || width == 32 || width == 16 || width == 8) {
		return 0, fmt.Errorf("width must be 64, 32, 16, or 8")
	}
	mask := uint64(1 << (width - 1))
	minus := u&mask != 0
	u &^= mask
	if minus {
		return -int64(u), nil
	}
	return int64(u), nil
}

func PrintMeta(m *Metadata) string {
	if m == nil {
		return ""
	}
	e := Encoder{Meta: *m}
	return string(e.MetaBytes(AllMeta))
}

var nilVals = [...]string{NilTenant, "", "\"\"", "0"}

// IsNilTenant checks if given tenant identifier considered as nil and should be skipped from querying settings and info.
func IsNilTenant(tenantID string) bool {
	tenantID = strings.TrimSpace(tenantID)
	for _, v := range nilVals {
		if tenantID == v {
			return true
		}
	}
	return false
}

var knownTypes = []string{
	String, Bigint, Int, Smallint, Tinyint, Double, Timestamp, Float, Date,
}

//nolint: gocyclo
func (m *Metadata) validate(noKey bool, logger log.FieldLogger) error {
	err := m.build(noKey)
	if err != nil {
		return err
	}

	if len(m.Columns) == 0 {
		return fmt.Errorf("#schema is required and must not be empty")
	}
	if len(m.KeyIdx) == 0 {
		return fmt.Errorf("#key is required and must not be empty")
	}

	if len(m.OrderIdx) > 0 && len(m.KeyIdx) == len(m.Columns) {
		logger.Warnf("#order provided but #key is implicit (all columns)")
	}
	if len(m.OrderIdx) > 0 {
		for _, o := range m.OrderIdx {
			for _, k := range m.KeyIdx {
				if k == o {
					logger.Warnf("#order column %q cannot be also in #key", m.Columns[o].Name)
				}
			}
		}
	}

	if len(m.OrderIdx) == 0 && len(m.KeyIdx) < len(m.Columns) {
		logger.Warn("#order is empty: make sure your entity is immutable, otherwise provide #order")
	}

	uniqueNames := map[string]struct{}{}
	for _, c := range m.Columns {
		lcName := strings.ToLower(c.Name)
		if strings.HasPrefix(c.Name, "_") || unicode.IsDigit([]rune(c.Name)[0]) {
			return fmt.Errorf("column name %q cannot start with underscore or digit", c.Name)
		}
		if _, ok := uniqueNames[lcName]; ok {
			return fmt.Errorf("column name %q is duplicate or only differs in case", lcName)
		}
		uniqueNames[lcName] = struct{}{}
		typeOk := false
		for _, t := range knownTypes {
			if t == c.Type {
				typeOk = true
				break
			}
		}
		if !typeOk {
			return fmt.Errorf("column %q type %q is unknown", c.Name, c.Type)
		}
		var camel = regexp.MustCompile("^[a-z]+(?:[A-Z][a-z]+)+$")
		if camel.MatchString(c.Name) {
			logger.Warn("column name uses camel case, snake case is recommended", log.String("column", c.Name))
		}
	}
	return nil
}

func (m *Metadata) printColumns(header string, idx []int, fn func(col Column) string) []byte {
	var buf bytes.Buffer
	buf.Grow(1024)
	buf.WriteString(header)
	buf.WriteByte(' ')
	sep := false
	for _, i := range idx {
		if sep {
			buf.WriteByte(CSVSeparator)
		}
		v := fn(m.Columns[i])
		sep = v != ""
		if v != "" {
			buf.WriteString(v)
		}
	}
	return buf.Bytes()
}