package M3u8Downloader

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseLines(t *testing.T) {
	t.Run("media playlist with key", func(t *testing.T) {
		lines := []string{
			"#EXTM3U",
			"#EXT-X-KEY:METHOD=AES-128,URI=\"https://example.com/key\",IV=0x1234",
			"segment1.ts",
			"segment2.ts",
		}

		got, err := parseLines(lines)
		if err != nil {
			t.Fatalf("parseLines() error = %v", err)
		}
		if len(got.Segments) != 2 {
			t.Fatalf("len(Segments) = %d, want 2", len(got.Segments))
		}
		if got.Segments[0].Key == nil || got.Segments[0].Key.URI != "https://example.com/key" {
			t.Fatalf("segment key = %#v", got.Segments[0].Key)
		}
	})

	t.Run("master playlist", func(t *testing.T) {
		lines := []string{
			"#EXTM3U",
			"#EXT-X-STREAM-INF:BANDWIDTH=123",
			"child.m3u8",
		}

		got, err := parseLines(lines)
		if err != nil {
			t.Fatalf("parseLines() error = %v", err)
		}
		if len(got.MasterPlaylistURIs) != 1 || got.MasterPlaylistURIs[0] != "child.m3u8" {
			t.Fatalf("MasterPlaylistURIs = %v", got.MasterPlaylistURIs)
		}
	})

	t.Run("invalid header", func(t *testing.T) {
		_, err := parseLines([]string{"#NOT-M3U"})
		if !errors.Is(err, errorMap[InvalidM3u8Exception]) {
			t.Fatalf("parseLines() error = %v, want %v", err, errorMap[InvalidM3u8Exception])
		}
	})

	t.Run("invalid key method", func(t *testing.T) {
		_, err := parseLines([]string{"#EXTM3U", "#EXT-X-KEY:METHOD=RSA,URI=\"https://example.com/key\""})
		if !errors.Is(err, errorMap[InvalidEXT_X_KEYMethod]) {
			t.Fatalf("parseLines() error = %v, want %v", err, errorMap[InvalidEXT_X_KEYMethod])
		}
	})
}

func TestParseLineParameters(t *testing.T) {
	line := `#EXT-X-KEY:METHOD=AES-128,URI="https://example.com/key",IV=0x1234`
	got := parseLineParameters(line)

	if got["METHOD"] != "AES-128" || got["URI"] != "https://example.com/key" || got["IV"] != "0x1234" {
		t.Fatalf("parseLineParameters() = %v", got)
	}
}

func TestAES128EncryptDecrypt(t *testing.T) {
	key := []byte("1234567890abcdef")
	iv := []byte("abcdef1234567890")
	plain := []byte("hello world")

	encrypted, err := AES128Encrypt(plain, key, iv)
	if err != nil {
		t.Fatalf("AES128Encrypt() error = %v", err)
	}

	decrypted, err := AES128Decrypt(encrypted, key, iv)
	if err != nil {
		t.Fatalf("AES128Decrypt() error = %v", err)
	}

	if !bytes.Equal(decrypted, plain) {
		t.Fatalf("decrypted = %q, want %q", decrypted, plain)
	}
}

func TestPKCS5Padding(t *testing.T) {
	plain := []byte("hello")
	padded := pkcs5Padding(plain, 8)
	if len(padded)%8 != 0 {
		t.Fatalf("len(padded) = %d, want multiple of 8", len(padded))
	}

	unpadded := pkcs5UnPadding(padded)
	if !bytes.Equal(unpadded, plain) {
		t.Fatalf("pkcs5UnPadding() = %q, want %q", unpadded, plain)
	}
}
