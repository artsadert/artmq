package server

import "testing"

func TestMatchTopic_Exact(t *testing.T) {
	if !matchTopic("a/b/c", "a/b/c") {
		t.Error("exact match should return true")
	}
	if matchTopic("a/b/c", "a/b/d") {
		t.Error("different topics should not match")
	}
}

func TestMatchTopic_SingleLevelWildcard(t *testing.T) {
	if !matchTopic("a/+/c", "a/b/c") {
		t.Error("+ should match single level")
	}
	if !matchTopic("a/+/c", "a/x/c") {
		t.Error("+ should match any single level value")
	}
	if matchTopic("a/+/c", "a/b/d") {
		t.Error("+ matches level but subsequent segments must still match")
	}
	if !matchTopic("+/b/c", "a/b/c") {
		t.Error("+ at start should match")
	}
}

func TestMatchTopic_MultiLevelWildcard(t *testing.T) {
	if !matchTopic("a/#", "a/b/c/d") {
		t.Error("# should match multiple levels")
	}
	if !matchTopic("a/#", "a/b") {
		t.Error("# should match a single trailing level")
	}
	if !matchTopic("#", "a/b/c") {
		t.Error("# alone should match any topic")
	}
}

func TestMatchTopic_LengthMismatch(t *testing.T) {
	if matchTopic("a/b", "a/b/c") {
		t.Error("filter shorter than topic should not match without wildcard")
	}
	if matchTopic("a/b/c", "a/b") {
		t.Error("filter longer than topic should not match")
	}
}

func TestMatchTopic_HashNotLast(t *testing.T) {
	if matchTopic("a/#/c", "a/b/c") {
		t.Error("# in the middle should not match")
	}
}

func TestMatchTopic_MultipleWildcards(t *testing.T) {
	if !matchTopic("+/+/+", "a/b/c") {
		t.Error("multiple + wildcards should match")
	}
	if matchTopic("+/+/+", "a/b") {
		t.Error("+ count must match topic level count")
	}
}
