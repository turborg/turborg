package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitOptions(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "trims whitespace around options",
			in:   " red, green ,blue ",
			want: []string{"red", "green", "blue"},
		},
		{
			name: "drops empty options",
			in:   "alpha,, , beta,",
			want: []string{"alpha", "beta"},
		},
		{
			name: "preserves internal whitespace",
			in:   "two words, spaced  inside",
			want: []string{"two words", "spaced  inside"},
		},
		{
			name: "empty input yields no options",
			in:   "",
			want: []string{},
		},
		{
			name: "whitespace only input yields no options",
			in:   " \t\n ",
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, splitOptions(tt.in))
		})
	}
}
