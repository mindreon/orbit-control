package bundle

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io/fs"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/contract"
)

func archive(t *testing.T, names []string, data []byte, mode fs.FileMode) []byte {
	t.Helper()
	var out bytes.Buffer
	z := zip.NewWriter(&out)
	for _, n := range names {
		h := &zip.FileHeader{Name: n, Method: zip.Deflate}
		h.SetMode(mode)
		w, e := z.CreateHeader(h)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Write(data); e != nil {
			t.Fatal(e)
		}
	}
	if e := z.Close(); e != nil {
		t.Fatal(e)
	}
	return out.Bytes()
}
func TestMaliciousArchives(t *testing.T) {
	for name, names := range map[string][]string{"slip": {"../escaped"},
		"absolute":  {"/tmp/file"},
		"windows":   {"C:/file"},
		"backslash": {"a\\..\\b"},
		"dot":       {"a/./b"},
		"deep": {strings.Repeat("a/",
			33) +
			"f"},
		"duplicate": {"same",
			"same"},
		"case": {"Name",
			"name"},
		"prefix": {"a",
			"a/b"},
		"newline": {"bad\nname"}} {
		t.Run(name, func(t *testing.T) {
			if _, e := Parse(archive(t, names, []byte("x"), 0644), DefaultLimits); e == nil {
				t.Fatal("accepted malicious archive")
			}
		})
	}
	for _, mode := range []fs.FileMode{fs.ModeSymlink | 0777, fs.ModeNamedPipe | 0644} {
		if _, e := Parse(archive(t, []string{"link"}, []byte("target"), mode), DefaultLimits); e == nil {
			t.Fatal("accepted special file")
		}
	}
	if _, e := Parse(archive(t, []string{"bomb"}, bytes.Repeat([]byte("0"), 1<<20), 0644), DefaultLimits); e == nil {
		t.Fatal("accepted zip bomb")
	}
	for name, l := range map[string]Limits{"count": {Files: 1,
		FileBytes:  100,
		TotalBytes: 100,
		TextBytes:  100,
		TextTotal:  100,
		Depth:      10,
		Ratio:      200},
		"file": {Files: 5,
			FileBytes:  1,
			TotalBytes: 100,
			TextBytes:  100,
			TextTotal:  100,
			Depth:      10,
			Ratio:      200},
		"total": {Files: 5,
			FileBytes:  100,
			TotalBytes: 1,
			TextBytes:  100,
			TextTotal:  100,
			Depth:      10,
			Ratio:      200},
		"text": {Files: 5,
			FileBytes:  100,
			TotalBytes: 100,
			TextBytes:  1,
			TextTotal:  100,
			Depth:      10,
			Ratio:      200}} {
		t.Run(name, func(t *testing.T) {
			if _, e := Parse(archive(t, []string{"a", "b"}, []byte("hello"), 0644), l); e == nil {
				t.Fatal("accepted over limit")
			}
		})
	}
}
func TestTarSafetyAndDeterminism(t *testing.T) {
	for _, kind := range []byte{tar.TypeSymlink, tar.TypeLink, tar.TypeChar} {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		tw := tar.NewWriter(gz)
		if e := tw.WriteHeader(&tar.Header{Name: "link", Typeflag: kind, Linkname: "outside", Mode: 0644}); e != nil {
			t.Fatal(e)
		}
		tw.Close()
		gz.Close()
		if _, e := Parse(b.Bytes(), DefaultLimits); e == nil {
			t.Fatal("accepted tar link/device")
		}
	}
	raw, e := Pack(
		map[string][]byte{
			"b":           []byte("second"),
			"a":           []byte("first"),
			".git/config": []byte("excluded"),
			"x.pyc":       []byte("excluded"),
		},
	)
	if e != nil {
		t.Fatal(e)
	}
	b, e := Parse(raw, DefaultLimits)
	if e != nil {
		t.Fatal(e)
	}
	if len(b.Files) != 2 || len(b.Excluded) != 2 {
		t.Fatal(b)
	}
	again, e := Parse(b.Archive, DefaultLimits)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(b.Archive, again.Archive) || b.Digest != again.Digest {
		t.Fatal("non-deterministic normalization")
	}
	hashes := map[string]string{}
	for _, f := range b.Files {
		hashes[f.Path] = f.Digest
	}
	expected, e := contract.BundleDigest(hashes)
	if e != nil || expected != b.Digest {
		t.Fatal("contract digest mismatch")
	}
}

func TestTarAndZipNormalizeEqually(t *testing.T) {
	content := []byte("instruction text")
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "AGENTS.md", Mode: 0755, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	fromTar, err := Parse(out.Bytes(), DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	fromZip, err := Parse(archive(t, []string{"AGENTS.md"}, content, 0644), DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	if fromTar.Digest != fromZip.Digest || !bytes.Equal(fromTar.Archive, fromZip.Archive) {
		t.Fatal("archive format/metadata changed content identity")
	}
}

func TestTarTrailingExpansionIsBounded(t *testing.T) {
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "a", Mode: 0644, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := gz.Write(bytes.Repeat([]byte{0}, 2<<20)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(out.Bytes(), DefaultLimits); err == nil {
		t.Fatal("accepted gzip bomb hidden after tar terminator")
	}
}

func TestUnicodeNormalizationCollision(t *testing.T) {
	for _, names := range [][]string{{"caf\u00e9.txt", "cafe\u0301.txt"}, {"caf\u00e9/a", "cafe\u0301"}} {
		if _, err := Parse(archive(t, names, []byte("x"), 0644), DefaultLimits); err == nil {
			t.Fatal("Unicode normalization collision accepted")
		}
	}
}
func TestPAXGlobalHeader(t *testing.T) {
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeXGlobalHeader,
		PAXRecords: map[string]string{"comment": "git archive"}}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "AGENTS.md", Mode: 0644, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := Parse(out.Bytes(), DefaultLimits)
	if err != nil || len(b.Files) != 1 {
		t.Fatal("git archive rejected", b, err)
	}
}
