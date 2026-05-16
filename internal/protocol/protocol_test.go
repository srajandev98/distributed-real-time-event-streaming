package protocol

import "testing"

// TestParseRequestValid covers the expected happy-path wire format.
func TestParseRequestValid(t *testing.T) {
	req, err := ParseRequest("V1|42|PRODUCE|orders key:value")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if req.Version != "V1" || req.CorrelationID != "42" || req.Command != "PRODUCE" {
		t.Fatalf("unexpected request metadata: %+v", req)
	}

	if len(req.Args) != 2 || req.Args[0] != "orders" || req.Args[1] != "key:value" {
		t.Fatalf("unexpected args: %+v", req.Args)
	}
}

// TestParseRequestInvalid verifies malformed requests are rejected.
func TestParseRequestInvalid(t *testing.T) {
	cases := []string{
		"",
		"V1|42|PRODUCE",
		"V2|42|PRODUCE|orders key:value",
		"V1||PRODUCE|orders key:value",
		"V1|42||orders key:value",
	}

	for _, tc := range cases {
		if _, err := ParseRequest(tc); err == nil {
			t.Fatalf("expected error for input %q", tc)
		}
	}
}

// TestResponses checks success/error response wire formatting.
func TestResponses(t *testing.T) {
	ok := Ok("7", "offset=3")
	if ok != "V1|7|OK|offset=3\n" {
		t.Fatalf("unexpected ok response: %q", ok)
	}

	err := Err("7", "BAD_REQUEST", "invalid partition")
	if err != "V1|7|ERR|BAD_REQUEST|invalid partition\n" {
		t.Fatalf("unexpected err response: %q", err)
	}
}

// TestParseInt verifies integer argument parsing used by handlers.
func TestParseInt(t *testing.T) {
	value, err := ParseInt("10", "offset")
	if err != nil || value != 10 {
		t.Fatalf("expected 10,nil got %d,%v", value, err)
	}

	if _, err := ParseInt("x", "offset"); err == nil {
		t.Fatalf("expected parse error")
	}
}
