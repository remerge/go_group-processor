package groupprocessor

import (
	"os"
	"testing"
	"time"

	"github.com/bobziuchkovski/cue"
	"github.com/bobziuchkovski/cue/collector"
	"github.com/bobziuchkovski/cue/format"
)

var testLog = cue.NewLogger("test")

func TestMain(m *testing.M) {
	level := cue.DEBUG

	formatter := format.Colorize(format.Formatf(
		"%v %v [%v:%v] %v",
		format.Time(time.RFC3339),
		format.Level,
		format.ContextName,
		format.SourceWithLine,
		format.HumanMessage,
	))

	cue.Collect(level, collector.Terminal{
		Formatter: formatter,
	}.New())

	os.Exit(m.Run())
}
