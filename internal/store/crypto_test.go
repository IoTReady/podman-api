package store

import (
	"bytes"
	"testing"
)

func testKey(b byte) [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = b
	}
	return k
}

func TestSealOpen_RoundTrip(t *testing.T) {
	key := testKey(0x11)
	plain := []byte(`{"password":"hunter2"}`)
	blob, err := seal(key, plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Equal(blob, plain) {
		t.Fatal("ciphertext equals plaintext")
	}
	got, err := open(key, blob)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plain)
	}
}

func TestOpen_WrongKey_Fails(t *testing.T) {
	blob, err := seal(testKey(0x11), []byte("secret"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := open(testKey(0x22), blob); err == nil {
		t.Fatal("open with wrong key should fail")
	}
}

func TestSeal_NonceUniqueness(t *testing.T) {
	key := testKey(0x11)
	a, _ := seal(key, []byte("x"))
	b, _ := seal(key, []byte("x"))
	if bytes.Equal(a, b) {
		t.Fatal("two seals of same plaintext are identical (nonce not random)")
	}
}

func TestOpen_TooShort_Fails(t *testing.T) {
	if _, err := open(testKey(0x11), []byte{0x00, 0x01}); err == nil {
		t.Fatal("open of too-short blob should fail")
	}
}

func TestOpen_NonceOnly_Fails(t *testing.T) {
	// A blob that is exactly the nonce length carries no authenticated
	// ciphertext; the length guard must reject it.
	nonceOnly := make([]byte, 12)
	if _, err := open(testKey(0x11), nonceOnly); err == nil {
		t.Fatal("open of nonce-only blob should fail")
	}
}
