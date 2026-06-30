package skill

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronSpec is a parsed 5-field cron expression (minute hour day-of-month
// month day-of-week), evaluated in UTC. It supports "*", numbers, comma lists,
// "a-b" ranges, and "*/n" / "a-b/n" steps — enough for the common cadences
// without a third-party dependency.
type cronSpec struct {
	minute  fieldSet
	hour    fieldSet
	dom     fieldSet
	month   fieldSet
	dow     fieldSet
	domStar bool // day-of-month was "*"
	dowStar bool // day-of-week was "*"
}

// fieldSet is the set of permitted values for one cron field.
type fieldSet map[int]struct{}

func (f fieldSet) has(v int) bool { _, ok := f[v]; return ok }

// parseCron parses a 5-field cron expression.
func parseCron(spec string) (*cronSpec, error) {
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: want 5 fields, got %d", len(fields))
	}
	c := &cronSpec{domStar: fields[2] == "*", dowStar: fields[4] == "*"}
	var err error
	if c.minute, err = parseField(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("cron minute: %w", err)
	}
	if c.hour, err = parseField(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("cron hour: %w", err)
	}
	if c.dom, err = parseField(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("cron day-of-month: %w", err)
	}
	if c.month, err = parseField(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("cron month: %w", err)
	}
	if c.dow, err = parseField(fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("cron day-of-week: %w", err)
	}
	return c, nil
}

// parseField parses one cron field into the set of values it permits.
func parseField(field string, lo, hi int) (fieldSet, error) {
	out := fieldSet{}
	for _, part := range strings.Split(field, ",") {
		rng := part
		step := 1
		if slash := strings.IndexByte(part, '/'); slash >= 0 {
			s, err := strconv.Atoi(part[slash+1:])
			if err != nil || s < 1 {
				return nil, fmt.Errorf("bad step %q", part)
			}
			step = s
			rng = part[:slash]
		}
		start, end := lo, hi
		switch {
		case rng == "*":
			// full range
		case strings.ContainsRune(rng, '-'):
			ab := strings.SplitN(rng, "-", 2)
			a, err1 := strconv.Atoi(ab[0])
			b, err2 := strconv.Atoi(ab[1])
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("bad range %q", part)
			}
			start, end = a, b
		default:
			v, err := strconv.Atoi(rng)
			if err != nil {
				return nil, fmt.Errorf("bad value %q", part)
			}
			start, end = v, v
		}
		if start < lo || end > hi || start > end {
			return nil, fmt.Errorf("field %q out of range [%d,%d]", part, lo, hi)
		}
		for v := start; v <= end; v += step {
			out[v] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty field")
	}
	return out, nil
}

// next returns the earliest minute strictly after `from` that the spec
// matches, evaluated in UTC. It scans minute-by-minute up to a bounded
// horizon (~366 days) so a never-matching spec returns a far-future time
// rather than looping forever.
func (c *cronSpec) next(from time.Time) time.Time {
	t := from.UTC().Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(1, 0, 1)
	for ; t.Before(limit); t = t.Add(time.Minute) {
		if c.matches(t) {
			return t
		}
	}
	return limit
}

// matches reports whether t satisfies the spec. Per cron convention, when both
// day-of-month and day-of-week are restricted (neither is "*"), a match on
// either is sufficient; otherwise both restricted fields must match.
func (c *cronSpec) matches(t time.Time) bool {
	if !c.minute.has(t.Minute()) || !c.hour.has(t.Hour()) || !c.month.has(int(t.Month())) {
		return false
	}
	domOK := c.dom.has(t.Day())
	dowOK := c.dow.has(int(t.Weekday()))
	if !c.domStar && !c.dowStar {
		return domOK || dowOK
	}
	return domOK && dowOK
}
