package tools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseClickHouseTime(t *testing.T) {
	const day = 24 * time.Hour
	const week = 7 * day

	testCases := []struct {
		name          string
		input         string
		expectedError bool
		expectedDelta time.Duration // Expected time difference from now
		isMonthCase   bool          // Special handling for month arithmetic
		isYearCase    bool          // Special handling for year arithmetic
	}{
		{
			name:          "now",
			input:         "now",
			expectedError: false,
			expectedDelta: 0,
		},
		{
			name:          "now-1h",
			input:         "now-1h",
			expectedError: false,
			expectedDelta: -1 * time.Hour,
		},
		{
			name:          "now-30m",
			input:         "now-30m",
			expectedError: false,
			expectedDelta: -30 * time.Minute,
		},
		{
			name:          "now-1d",
			input:         "now-1d",
			expectedError: false,
			expectedDelta: -24 * time.Hour,
		},
		{
			name:          "now-1w",
			input:         "now-1w",
			expectedError: false,
			expectedDelta: -week,
		},
		{
			name:          "now-24h",
			input:         "now-24h",
			expectedError: false,
			expectedDelta: -24 * time.Hour,
		},
		{
			name:          "invalid format",
			input:         "yesterday",
			expectedError: true,
		},
		{
			name:          "empty string",
			input:         "",
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			result, err := parseClickHouseTime(tc.input)

			if tc.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tc.input == "now" {
				// For "now", the result should be very close to the current time
				// Allow a small tolerance for execution time
				diff := result.Sub(now)
				assert.Less(t, diff.Abs(), 2*time.Second, "Time difference should be less than 2 seconds")
			} else if tc.isMonthCase {
				// For month calculations, use proper calendar arithmetic
				expected := now.AddDate(0, -1, 0)
				diff := result.Sub(expected)
				assert.Less(t, diff.Abs(), 2*time.Second, "Time difference should be less than 2 seconds")
			} else if tc.isYearCase {
				// For year calculations, use proper calendar arithmetic
				expected := now.AddDate(-1, 0, 0)
				diff := result.Sub(expected)
				assert.Less(t, diff.Abs(), 2*time.Second, "Time difference should be less than 2 seconds")
			} else {
				// For other relative times, compare with the expected delta from now
				expected := now.Add(tc.expectedDelta)
				diff := result.Sub(expected)
				assert.Less(t, diff.Abs(), 2*time.Second, "Time difference should be less than 2 seconds")
			}
		})
	}
}

func TestEnforceRowLimit(t *testing.T) {
	testCases := []struct {
		name           string
		requestedLimit int
		expectedLimit  int
	}{
		{
			name:           "zero limit defaults to default",
			requestedLimit: 0,
			expectedLimit:  DefaultClickHouseLimit,
		},
		{
			name:           "negative limit defaults to default",
			requestedLimit: -100,
			expectedLimit:  DefaultClickHouseLimit,
		},
		{
			name:           "valid limit within bounds",
			requestedLimit: 500,
			expectedLimit:  500,
		},
		{
			name:           "limit at default",
			requestedLimit: DefaultClickHouseLimit,
			expectedLimit:  DefaultClickHouseLimit,
		},
		{
			name:           "limit at max",
			requestedLimit: MaxClickHouseLimit,
			expectedLimit:  MaxClickHouseLimit,
		},
		{
			name:           "limit exceeding max gets capped",
			requestedLimit: MaxClickHouseLimit + 1000,
			expectedLimit:  MaxClickHouseLimit,
		},
		{
			name:           "very large limit gets capped",
			requestedLimit: 1000000,
			expectedLimit:  MaxClickHouseLimit,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := enforceRowLimit(tc.requestedLimit)
			assert.Equal(t, tc.expectedLimit, result)
		})
	}
}
