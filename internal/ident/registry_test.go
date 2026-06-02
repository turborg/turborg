package ident

import "testing"

func TestRegistrySetLookupClear(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Lookup(51440); ok {
		t.Fatal("empty registry should miss")
	}
	r.Set(51440, "tf1dcv74w")
	if id, ok := r.Lookup(51440); !ok || id != "tf1dcv74w" {
		t.Fatalf("Lookup after Set = %q,%v; want tf1dcv74w,true", id, ok)
	}
	r.Clear(51440)
	if _, ok := r.Lookup(51440); ok {
		t.Fatal("Lookup after Clear should miss")
	}
}

func TestRegistryIgnoresZeroPortAndEmptyIdent(t *testing.T) {
	r := NewRegistry()
	// A half-open dial (LocalPort 0) or a connector with no username must not
	// poison the map — a bogus 0→"" entry could otherwise mask a real lookup.
	r.Set(0, "tf1dcv74w")
	r.Set(51440, "")
	if _, ok := r.Lookup(0); ok {
		t.Fatal("zero port must not register")
	}
	if _, ok := r.Lookup(51440); ok {
		t.Fatal("empty ident must not register")
	}
}

func TestRegistryClearZeroIsNoop(t *testing.T) {
	r := NewRegistry()
	r.Set(51440, "tf1dcv74w")
	r.Clear(0) // must not touch real entries
	if _, ok := r.Lookup(51440); !ok {
		t.Fatal("Clear(0) must not remove a real entry")
	}
}
