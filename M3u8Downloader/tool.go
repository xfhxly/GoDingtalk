package M3u8Downloader

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

// 全局HTTP客户端，复用连接池。
// 默认 30s 超时；main 包可通过 SetHTTPClient 注入自定义客户端
// （例如使用 -httpTimeout 配置），使超时设置对 ts 分片下载同样生效。
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	},
}

// SetHTTPClient 替换下载器使用的全局 HTTP 客户端。
// 仅应在启动阶段、开启下载 goroutine 之前调用，避免与并发请求产生竞争。
func SetHTTPClient(c *http.Client) {
	if c != nil {
		httpClient = c
	}
}

// processNum 将分片索引转换为带前导零的 ts 文件名（如 0000.ts）。
// 使用 %04d：索引 <10000 时为 4 位，超出时自动扩展位数，不再有上限。
func processNum(n int) []byte {
	return []byte(fmt.Sprintf("%04d.ts", n))
}

// getAllNonDirectoryFile 获取所有非目录文件，并按文件名排序以保证合并顺序确定。
func getAllNonDirectoryFile(pathName string) ([]string, error) {
	rd, err := os.ReadDir(pathName)
	if err != nil {
		return nil, errorMap[ReadDirectoryException]
	}
	Files := make([]string, 0, len(rd))
	for i := 0; i < len(rd); i++ {
		if !rd[i].IsDir() {
			Files = append(Files, path.Join(pathName, rd[i].Name()))
		}
	}
	sort.Strings(Files)
	return Files, nil
}

// httpGet 发起 GET 请求，返回响应体、异常类型以及具体错误。
// 不再写入全局 errorMap —— 多个下载 goroutine 并发调用时同时写 map 会触发
// concurrent map writes panic，因此错误详情通过返回值传出，由调用方按需记录。
func httpGet(url string) (io.ReadCloser, DownloadExceptionType, error) {
	resp, err := httpClient.Get(url)
	if err != nil {
		if strings.HasSuffix(err.Error(), "no such host") {
			return nil, NetworkException, err
		}
		return nil, UnexpectedException, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, HttpException, fmt.Errorf("[HttpError]: status code %d", resp.StatusCode)
	}
	return resp.Body, NoException, nil
}

// mergeFile 合并文件主函数
func mergeFile(path string, fileList []string, saveName string) error {
	var (
		buffer strings.Builder
		err    error
		movie  *os.File
	)
	buffer.WriteString(path)
	buffer.WriteString(saveName)
	movie, err = os.OpenFile(buffer.String(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer movie.Close()

	var tsFile *os.File
	// 使用流式复制，避免一次性读取整个文件到内存
	for i := 0; i < len(fileList); i++ {
		tsFile, err = os.Open(fileList[i])
		if err != nil {
			return err
		}

		// 使用 io.Copy 进行流式复制，内存占用小
		_, err = io.Copy(movie, tsFile)
		tsFile.Close()
		if err != nil {
			return err
		}

		// 复制完成后删除源文件
		err = os.Remove(fileList[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// getUnixTimeAndToByte 根据当前时间戳生成默认名称
func getUnixTimeAndToByte() string {
	// 直接使用标准库的转换函数
	return fmt.Sprintf("%d", time.Now().Unix())
}

// ResolveURL 处理Url
func ResolveURL(u *url.URL, p string) string {
	if strings.HasPrefix(p, "https://") || strings.HasPrefix(p, "http://") {
		return p
	}
	var baseURL string
	if strings.Index(p, "/") == 0 {
		baseURL = u.Scheme + "://" + u.Host
	} else {
		tU := u.String()
		baseURL = tU[0:strings.LastIndex(tU, "/")]
	}
	return baseURL + path.Join("/", p)
}

// PathExists 判断路径是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// CheckAndCreatDirectory 判断目录是否存在，若不存在则创建（含必要的父目录）。
// 使用 MkdirAll，支持嵌套保存目录（如 downloads/video/）。
func CheckAndCreatDirectory(path string) error {
	return os.MkdirAll(path, 0755)
}
