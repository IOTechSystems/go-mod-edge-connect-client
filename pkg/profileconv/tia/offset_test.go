// Copyright (C) 2026 IOTech Ltd

package tia

import "testing"

// The offset tracker is the whole reason this converter can exist: a DB source
// file carries no addresses, so every resource's start is derived by accumulating
// type sizes. These tests pin the S7 non-optimised layout rules directly on the
// tracker, independent of parsing.

// A 1-byte type needs no alignment, so consecutive ones fill both an even and an
// odd address. A 2-byte type then has to skip the odd byte.
func TestAllocOneByteTypesNeedNoAlignment(t *testing.T) {
	off := &offsetTracker{}

	if got := off.alloc(1, 0); got != 0 {
		t.Errorf("first 1-byte alloc: got %d, want 0", got)
	}
	if got := off.alloc(1, 0); got != 1 {
		t.Errorf("second 1-byte alloc: got %d, want 1 (odd address is legal)", got)
	}
	if got := off.alloc(2, 2); got != 2 {
		t.Errorf("word-aligned alloc: got %d, want 2", got)
	}
	off.alloc(1, 0) // leaves the cursor odd, at 5
	if got := off.alloc(2, 2); got != 6 {
		t.Errorf("word align from odd cursor: got %d, want 6", got)
	}
}

// Bools are bit-packed eight to a byte, spilling into the next byte
// on the ninth.
func TestAllocBoolPacksEightPerByte(t *testing.T) {
	off := &offsetTracker{}

	for i := 0; i < 8; i++ {
		gotByte, gotBit := off.allocBool()
		if gotByte != 0 || gotBit != i {
			t.Errorf("bool %d: got byte %d bit %d, want byte 0 bit %d", i, gotByte, gotBit, i)
		}
	}
	gotByte, gotBit := off.allocBool()
	if gotByte != 1 || gotBit != 0 {
		t.Errorf("ninth bool: got byte %d bit %d, want byte 1 bit 0", gotByte, gotBit)
	}
}

// A non-Bool type closes a partially used bool byte before allocating, then
// still honours its own alignment: one bool leaves byte 0 partly used, so an Int
// cannot start until byte 2. A 1-byte type closes the byte too, but does not
// align, so it takes byte 1.
func TestAllocAfterPartialBoolByte(t *testing.T) {
	t.Run("Int aligns", func(t *testing.T) {
		off := &offsetTracker{}
		off.allocBool()
		if got := off.alloc(2, 2); got != 2 {
			t.Errorf("got %d, want 2", got)
		}
	})
	t.Run("SInt takes the next byte", func(t *testing.T) {
		off := &offsetTracker{}
		off.allocBool()
		if got := off.alloc(1, 0); got != 1 {
			t.Errorf("got %d, want 1", got)
		}
	})
}

// String[n] occupies 2+n bytes (a 2-byte header plus the content) rounded up to
// even, so an odd declared length wastes a byte.
func TestAllocStringPadsTotalToEven(t *testing.T) {
	tests := []struct {
		name      string
		maxLen    int
		wantStart int
		wantNext  int // where the following allocation lands
	}{
		{"even total", 10, 0, 12},       // 2+10 = 12, already even
		{"odd total padded", 9, 0, 12},  // 2+9 = 11 -> 12
		{"default length", 254, 0, 256}, // 2+254 = 256
		{"odd default", 253, 0, 256},    // 2+253 = 255 -> 256
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			off := &offsetTracker{}
			if got := off.allocString(tt.maxLen); got != tt.wantStart {
				t.Errorf("start: got %d, want %d", got, tt.wantStart)
			}
			if off.Byte != tt.wantNext {
				t.Errorf("cursor after String[%d]: got %d, want %d", tt.maxLen, off.Byte, tt.wantNext)
			}
		})
	}
}

func TestToWordBoundaryDiscardsPartialBoolByte(t *testing.T) {
	off := &offsetTracker{}
	off.alloc(2, 2) // an Int member: bytes 0-1
	off.allocBool() // a Bool member: byte 2, bit 0
	off.toWordBoundary()

	// Closing byte 2 leaves the cursor at 3, which is odd, so the word-align
	// step pushes it to 4 — a struct always ends on an even boundary.
	if off.Byte != 4 || off.Bit != 0 {
		t.Fatalf("after toWordBoundary: got byte %d bit %d, want byte 4 bit 0", off.Byte, off.Bit)
	}
	if got := off.alloc(2, 2); got != 4 {
		t.Errorf("member after struct: got %d, want 4", got)
	}
}

// An already word-aligned cursor must not gain padding.
func TestToWordBoundaryIsIdempotentWhenAligned(t *testing.T) {
	off := &offsetTracker{}
	off.alloc(4, 2) // cursor -> 4, even, no partial bool byte
	off.toWordBoundary()

	if off.Byte != 4 {
		t.Errorf("aligned cursor: got %d, want 4 (no padding)", off.Byte)
	}
}

// An array occupies element_size x count with no per-element padding: only the
// array's own start is word-aligned, so later elements can sit on odd addresses.
func TestAllocArrayHasNoPerElementPadding(t *testing.T) {
	off := &offsetTracker{}
	off.alloc(1, 0) // push the cursor to an odd address

	start := off.alloc(1*3, 2) // Array[0..2] of SInt
	if start != 2 {
		t.Fatalf("array start: got %d, want 2 (word-aligned)", start)
	}
	// Elements are at 2, 3, 4 — the middle one is odd, which is legal.
	if off.Byte != 5 {
		t.Errorf("cursor after 3x1-byte array: got %d, want 5", off.Byte)
	}
}

// A WString aligns and closes a pending bool byte like any other sized type.
// Every other test leads with an Int, which leaves the cursor already even with
// no bool byte open, so neither step has anything to do and both would pass
// unnoticed if they were dropped.
func TestAllocWStringAlignsAndClosesBoolByte(t *testing.T) {
	t.Run("from an odd cursor", func(t *testing.T) {
		off := &offsetTracker{}
		off.alloc(1, 0) // a 1-byte type leaves the cursor at 1

		if got := off.allocWString(5); got != 2 {
			t.Errorf("start: got %d, want 2 (word-aligned)", got)
		}
		if off.Byte != 16 { // 2 + (2+5)*2
			t.Errorf("cursor: got %d, want 16", off.Byte)
		}
	})

	t.Run("after a bool", func(t *testing.T) {
		off := &offsetTracker{}
		off.allocBool() // byte 0 is now partly used

		if got := off.allocWString(5); got != 2 {
			t.Errorf("start: got %d, want 2 (the bool byte is closed, then aligned)", got)
		}
	})
}

// A Bool array must close a pending bool byte before claiming its own. Without
// that, its first element resolves to the same PLC bit as the preceding scalar
// Bool and writing either corrupts the other.
func TestToWordBoundaryClosesPendingBoolByteForAnArray(t *testing.T) {
	off := &offsetTracker{}
	off.allocBool() // byte 0 bit 0 belongs to a scalar Bool

	off.toWordBoundary()
	if off.Byte != 2 || off.Bit != 0 {
		t.Errorf("bool array start: got byte %d bit %d, want byte 2 bit 0", off.Byte, off.Bit)
	}
}
