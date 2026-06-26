package M3u8Downloader

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type testRoundTripFunc func(*http.Request) (*http.Response, error)

func (f testRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestGetAllNonDirectoryFile(t *testing.T) {
	tmpDir := t.TempDir()
	fileA := filepath.Join(tmpDir, "a.txt")
	fileB := filepath.Join(tmpDir, "b.txt")
	if err := os.WriteFile(fileA, []byte("a"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fileB, []byte("b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(tmpDir, "sub"), 0755); err != nil {
		t.Fatal(err)
	}

	files, err := getAllNonDirectoryFile(tmpDir)
	if err != nil {
		t.Fatalf("getAllNonDirectoryFile() error = %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("len(files) = %d, want 2", len(files))
	}
}

func TestHTTPHelpers(t *testing.T) {
	originalClient := httpClient
	t.Cleanup(func() { httpClient = originalClient })

	httpClient = &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("body")),
			Header:     make(http.Header),
		}, nil
	})}

	body, exception, _ := httpGet("https://example.com")
	if exception != NoException {
		t.Fatalf("httpGet() exception = %v", exception)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || string(data) != "body" {
		t.Fatalf("httpGet() body = %q, err = %v", data, err)
	}

	httpClient = &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Body:       io.NopCloser(strings.NewReader("bad")),
			Header:     make(http.Header),
		}, nil
	})}
	_, exception, _ = httpGet("https://example.com")
	if exception != HttpException {
		t.Fatalf("httpGet() exception = %v, want %v", exception, HttpException)
	}

	httpClient = &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("lookup example.invalid: no such host")
	})}
	_, exception, _ = httpGet("https://example.invalid")
	if exception != NetworkException {
		t.Fatalf("httpGet() exception = %v, want %v", exception, NetworkException)
	}
}

func TestDownloaderHTTPAndMergeFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/playlist.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\nsegment1.ts\nsegment2.ts\n")
		case "/segment1.ts":
			_, _ = w.Write([]byte("A"))
		case "/segment2.ts":
			_, _ = w.Write([]byte("B"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	md := NewDownloader().(*m3u8downloader)
	md.SetUrl(server.URL + "/playlist.m3u8")
	md.SetMovieName("movie")
	md.SetSaveDirectory(tmpDir)
	md.SetNumOfThread(2)

	if err := md.Download(); err != nil {
		t.Fatalf("Download() error = %v", err)
	}
	if len(md.suffixList) != 2 {
		t.Fatalf("len(suffixList) = %d, want 2", len(md.suffixList))
	}
	if err := md.MergeFile(); err != nil {
		t.Fatalf("MergeFile() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, "movie.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "AB" {
		t.Fatalf("merged file = %q, want %q", got, "AB")
	}
}

func TestDefaultDownloadAndShowBar(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/playlist.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\nsegment1.ts\n")
		case "/segment1.ts":
			_, _ = w.Write([]byte("A"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	md := NewDownloader().(*m3u8downloader)
	md.SetUrl(server.URL + "/playlist.m3u8")
	md.SetMovieName("movie")
	md.SetSaveDirectory(tmpDir)
	md.SetNumOfThread(1)
	md.SetIfShowTheBar(true)

	if !md.DefaultDownload() {
		t.Fatal("DefaultDownload() = false, want true")
	}

	if _, err := os.Stat(filepath.Join(tmpDir, "movie.ts")); err != nil {
		t.Fatalf("merged output missing: %v", err)
	}
}

func TestParseM3u8FileEncryptedAndMergeFileInDir(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/master.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=123\nmedia.m3u8\n")
		case "/media.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\nsegment.ts\n")
		case "/segment.ts":
			_, _ = w.Write([]byte("segment"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	md := NewDownloader().(*m3u8downloader)
	result, err := md.ParseM3u8FileEncrypted(server.URL + "/master.m3u8")
	if err != nil {
		t.Fatalf("ParseM3u8FileEncrypted() error = %v", err)
	}
	if len(result.M3u8.Segments) != 1 || result.M3u8.Segments[0].URI != "segment.ts" {
		t.Fatalf("ParseM3u8FileEncrypted() result = %+v", result.M3u8.Segments)
	}

	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "0000.ts"), []byte("one"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "0001.ts"), []byte("two"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := md.MergeFileInDir(tmpDir+string(os.PathSeparator), "merged.ts"); err != nil {
		t.Fatalf("MergeFileInDir() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, "merged.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "onetwo" {
		t.Fatalf("MergeFileInDir() output = %q", got)
	}
}

func TestMiscDownloaderHelpers(t *testing.T) {
	cfg := &DownloadConfig{VideoName: "movie.ts", SaveDirectory: "out/"}
	md := NewDownloaderWithConfig(cfg).(*m3u8downloader)
	if md.config != cfg {
		t.Fatal("NewDownloaderWithConfig() did not use provided config")
	}

	bodyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("payload"))
	}))
	defer bodyServer.Close()

	body := md.httpGetBodyToByte(bodyServer.URL)
	if got := string(body); got != "payload" {
		t.Fatalf("httpGetBodyToByte() = %q", got)
	}

	md.config.TotalNum = 3
	md.taskChannel = make(IntChannel, 3)
	md.publishDownloadTask()
	var got []int
	for v := range md.taskChannel {
		got = append(got, v)
	}
	if fmt.Sprint(got) != "[0 1 2]" {
		t.Fatalf("publishDownloadTask() = %v", got)
	}

	if reFind(len([]byte("https://example.com/a/b"))-1, []byte("https://example.com/a/b")) <= 0 {
		t.Fatal("reFind() returned invalid index")
	}
}

// TestDownloadConcurrentFailureNoPanic 验证多线程并发下载全部失败时，
// 不会因并发写入共享异常状态（原 errorMap 写入 / md.exception 裸写）而 panic 或产生 data race。
// 这是第 1 条修复的回归保护：错误累计达到致命阈值后，Download 返回 error 而非崩溃。
func TestDownloadConcurrentFailureNoPanic(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/playlist.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\n")
			for i := 0; i < 50; i++ {
				_, _ = fmt.Fprintf(w, "seg%d.ts\n", i)
			}
		default:
			// 所有分片请求都返回 500，触发多 goroutine 并发 HttpException
			http.Error(w, "server error", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	md := NewDownloader().(*m3u8downloader)
	md.SetUrl(server.URL + "/playlist.m3u8")
	md.SetMovieName("fail-movie")
	md.SetSaveDirectory(tmpDir)
	md.SetNumOfThread(8)

	err := md.Download()
	if err == nil {
		t.Fatal("Download() error = nil, want non-nil when all segments fail")
	}
}
