// Copyright (C) 2026 IOTech Ltd

package tia

// offsetTracker tracks the byte + bit cursor for S7 non-optimised DB layout.
//
// Rules:
//   - Bool variables are bit-packed: up to 8 per byte, bitIndex 0 to 7.
//   - Any non-Bool type ends the current bool-byte and starts at the next whole byte.
//   - Types of 2+ bytes must start at an even (word) address.
//   - Structs end with word-alignment padding.
//   - String[n] occupies (2 + n) bytes, padded to an even number.
//   - WString[n] occupies (2 + n) * 2 bytes, which is always even.
type offsetTracker struct {
	Byte int
	Bit  int // 0–7
}

func (o *offsetTracker) closeBoolByte() {
	if o.Bit > 0 {
		o.Byte++
		o.Bit = 0
	}
}

func (o *offsetTracker) wordAlign() {
	if o.Byte%2 != 0 {
		o.Byte++
	}
}

// toWordBoundary ends any partial bool byte and moves to the next even address.
// alloc does the same conditionally: a 1-byte type may sit at an odd address.
func (o *offsetTracker) toWordBoundary() {
	o.closeBoolByte()
	o.wordAlign()
}

func (o *offsetTracker) allocBool() (byteOff, bitIdx int) {
	byteOff, bitIdx = o.Byte, o.Bit
	o.Bit++
	if o.Bit == 8 {
		o.Bit = 0
		o.Byte++
	}
	return
}

func (o *offsetTracker) alloc(size, align int) int {
	o.closeBoolByte()
	if align >= 2 {
		o.wordAlign()
	}
	out := o.Byte
	o.Byte += size
	return out
}

func (o *offsetTracker) allocString(maxLen int) int {
	o.toWordBoundary()
	out := o.Byte
	total := 2 + maxLen
	o.Byte += total + (total % 2) // pad to even
	return out
}

func (o *offsetTracker) allocWString(maxLen int) int {
	o.toWordBoundary()
	out := o.Byte
	o.Byte += (2 + maxLen) * 2
	return out
}
