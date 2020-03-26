package cursors

type TagKeys struct {
	Measurement string
	TagKeys     []string
}

type TagKeysCursor interface {
	// Next advances the TagKeysCursor to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() TagKeys

	Stats() CursorStats
}

type TagValues struct {
	Measurement string
	TagValues   []string
}

type TagValuesCursor interface {
	// Next advances the TagValuesCursor to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() TagValues

	Stats() CursorStats
}

// FieldType represents the primitive field data types available in tsm.
type FieldType int

const (
	// Float means the data type is a float.
	Float FieldType = 0
	// Integer means the data type is an integer.
	Integer FieldType = 1
	// Unsigned means the data type is an unsigned integer.
	Unsigned FieldType = 2
	// Boolean means the data type is a boolean.
	Boolean FieldType = 3
	// String means the data type is a string of text.
	String FieldType = 4
)

type MeasurementField struct {
	Key  string
	Type FieldType
}

type MeasurementFields struct {
	Measurement string
	Fields      []MeasurementField
}

type MeasurementFieldsCursor interface {
	// Next advances the MeasurementFieldsCursor to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() MeasurementFields

	Stats() CursorStats
}
