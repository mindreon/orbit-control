package usage

// Meter identifies a quantity unit; quantities may only be aggregated per meter.
type Meter string

const (
	TokensInput      Meter = "tokens.input"
	TokensOutput     Meter = "tokens.output"
	TokensCacheRead  Meter = "tokens.cache_read"
	TokensCacheWrite Meter = "tokens.cache_write"
)

func Meters() []Meter {
	return []Meter{TokensInput, TokensOutput, TokensCacheRead, TokensCacheWrite}
}
