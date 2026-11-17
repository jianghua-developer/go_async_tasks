package util

import "testing"

func TestQueueNameConversions(t *testing.T) {
	cases := []struct {
		in     string
		qName  string
		dqName string
		xqName string
	}{
		{"default", "default", "default.DQ", "default.XQ"},
		{"default.DQ", "default", "default.DQ", "default.XQ"},
		{"default.XQ", "default", "default.DQ", "default.XQ"},
	}

	for _, c := range cases {
		if got := QName(c.in); got != c.qName {
			t.Errorf("QName(%q) = %q, want %q", c.in, got, c.qName)
		}
		if got := DQName(c.in); got != c.dqName {
			t.Errorf("DQName(%q) = %q, want %q", c.in, got, c.dqName)
		}
		if got := XQName(c.in); got != c.xqName {
			t.Errorf("XQName(%q) = %q, want %q", c.in, got, c.xqName)
		}
	}
}

func TestQueueNameIdempotent(t *testing.T) {
	for _, name := range []string{"orders", "orders.DQ", "orders.XQ"} {
		if got := QName(DQName(name)); got != "orders" {
			t.Errorf("QName(DQName(%q)) = %q, want orders", name, got)
		}
		if got := QName(XQName(name)); got != "orders" {
			t.Errorf("QName(XQName(%q)) = %q, want orders", name, got)
		}
	}
}
