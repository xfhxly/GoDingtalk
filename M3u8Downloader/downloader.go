package M3u8Downloader

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	CryptMethodAES        CryptMethod = "AES-128"
	CryptMethodNONE       CryptMethod = "NONE"
	defaultNumberOfThread             = 10
	// SuffixTs 下载文件的后缀（TS 格式，后续通过 ffmpeg 转换为 mp4）
	SuffixTs = ".ts"
)

const (
	NoException DownloadExceptionType = iota
	UrlException
	IOException
	NetworkException
	UnexpectedException
	ReadDirectoryException
	InvalidM3u8Exception
	InvalidEXT_X_KEY
	InvalidEXT_X_KEYMethod
	HttpException
	NoM3u8SegmentException
	DecrytTSFailed
)

// errorMap 异常类型 → 默认提示文案的只读映射。
// 注意：该 map 仅在 init 阶段写入，之后只读。
// 运行期不再向其写入具体的错误信息（那会引发并发 map 写入 panic），
// 具体错误通过 httpGet 的返回值沿调用链传递，记录在 m3u8downloader.lastErr。
var (
	defaultSaveDirectory = "./video/"
	errorMap             = map[DownloadExceptionType]error{}
)

// init 初始化异常默认文案
func init() {
	errorMap[UrlException] = errors.New("[URLException]: Invalid URL format, please check your URL")
	errorMap[IOException] = errors.New("[IOException]: Failed to read or write file")
	errorMap[ReadDirectoryException] = errors.New("[ReadDirectoryException]: Failed to read directory")
	errorMap[NetworkException] = errors.New("[NetworkException]: Network connection failed")
	errorMap[UnexpectedException] = errors.New("[UnexpectedException]: Unexpected error during HTTP request")
	errorMap[HttpException] = errors.New("[HttpException]: HTTP request returned non-200 status")
	errorMap[InvalidM3u8Exception] = errors.New("[InvalidM3u8Exception]: Invalid M3U8 file format")
	errorMap[InvalidEXT_X_KEY] = errors.New("[InvalidEXT_X_KEY]: Invalid encryption key in M3U8 file")
	errorMap[InvalidEXT_X_KEYMethod] = errors.New("[InvalidEXT_X_KEYMethod]: Unsupported encryption method")
	errorMap[NoM3u8SegmentException] = errors.New("[NoM3u8SegmentException]: No video segments found in M3U8 file")
	errorMap[DecrytTSFailed] = errors.New("[DecryptTSFailed]: Failed to decrypt video segment")
}

// 自定义类型
type (
	IntChannel chan int
	// downloadCallback 单个分片下载完成后的处理回调（写盘）
	downloadCallback      func(index, threadId int, body []byte)
	DownloadExceptionType int
)

// M3u8Downloader 下载器接口对象
type M3u8Downloader interface {
	// DefaultDownload 默认下载方式，建议使用，方便快捷,下载为ts文件，然后合并，将会返回一个下载状态
	DefaultDownload() bool
	// ParseM3u8FileEncrypted 解析加密m3u8文件
	ParseM3u8FileEncrypted(link string) (*Result, error)
	// Download 根据之前的配置开始执行下载
	Download() error
	// SetUrl 设置url
	SetUrl(url string)
	// SetIfShowTheBar 是否显示进度条
	SetIfShowTheBar(ifShow bool)
	// SetNumOfThread 设置下载线程的数量
	SetNumOfThread(num int)
	// SetMovieName 设置视频的文件名
	SetMovieName(videoName string)
	// SetSaveDirectory 设置保存目录
	SetSaveDirectory(targetDir string)
	// MergeFile 默认合并文件
	MergeFile() error
	// MergeFileInDir 将合并后的视频文件保存到目录dir中
	MergeFileInDir(path string, saveName string) error
}

// m3u8downloader 下载器对象结构体
type m3u8downloader struct {
	// config 下载配置
	config *DownloadConfig
	// taskChannel 发布下载任务的管道
	taskChannel IntChannel
	// suffixList 需要合并的url列表
	suffixList []string
	//waitGroup 用来保证线程同步
	waitGroup *sync.WaitGroup
	// buffer 用于构造一些字符串所需要使用的缓冲区
	buffer []strings.Builder
	//exception 下载中的异常类型（并发访问，统一经 config.errMutex 保护）
	exception DownloadExceptionType
	// lastErr 最近一次异常的具体错误信息，配合 exception 一起返回给调用方
	lastErr error
	// m3u8ParseResult 对于m3u8解析的结果
	m3u8ParseResult *Result
	// successChan
	successChan IntChannel
}

// DownloadConfig 下载配置对象
// 因为最后还有很多拼接字符串的地方，所以我们尽可能使用[]byte和StringBuilder
// 以减少不必要的开销
type DownloadConfig struct {
	// Url 下载链接
	Url []byte
	// noSuffixUrl 经过处理后得到无后缀的url
	noSuffixUrl []byte
	// NumOfThreads 下载的线程数
	NumOfThreads int
	// VideoName 保存的视频名称
	VideoName string
	// SaveDirectory 保存视频的目录
	SaveDirectory string
	// ifShowBar 是否在控制台显示进度条
	ifShowBar bool
	// errCount 下载出错的数量
	errCount int
	// errMutex 保护 errCount / exception / lastErr 的互斥锁
	errMutex sync.Mutex
	// completeCount 下载完成的数量（使用原子操作保证线程安全）
	completeCount int64
	// TotalNum 总共的下载数量
	TotalNum int
}

// M3u8 解析m3u8文件后的内容的集合
type M3u8 struct {
	Segments           []*Segment
	MasterPlaylistURIs []string
}

// Segment 包含了.m3u8文件中每一条url
type Segment struct {
	URI string
	Key *Key
}

// Key 对于加密结构体的数据载体
type Key struct {
	URI    string
	IV     string
	key    string
	Method CryptMethod
}

// Result m3u8文件的处理结果对象
type Result struct {
	URL  *url.URL
	M3u8 *M3u8
	Keys map[*Key]string
}

// NewDownloader 创建一个新下载器对象
func NewDownloader() M3u8Downloader {
	return defaultConstructor(&DownloadConfig{
		NumOfThreads:  defaultNumberOfThread,
		ifShowBar:     false,
		SaveDirectory: defaultSaveDirectory,
		Url:           nil,
	})
}

// NewDownloaderWithConfig 使用自定义配置创建下载器对象
func NewDownloaderWithConfig(config *DownloadConfig) M3u8Downloader {
	return defaultConstructor(config)
}

// defaultConstructor 默认构造函数
func defaultConstructor(config *DownloadConfig) M3u8Downloader {
	return &m3u8downloader{
		config:      config,
		buffer:      make([]strings.Builder, defaultNumberOfThread),
		exception:   NoException,
		waitGroup:   &sync.WaitGroup{},
		taskChannel: nil,
		successChan: nil,
	}
}

// markException 线程安全地记录异常类型与具体错误。
// 下载过程存在多个 goroutine 并发，exception / lastErr 的读写必须经 errMutex 保护。
func (md *m3u8downloader) markException(exc DownloadExceptionType, err error) {
	md.config.errMutex.Lock()
	defer md.config.errMutex.Unlock()
	md.exception = exc
	if err != nil {
		md.lastErr = err
	}
}

// httpGetBodyToByte 以byte数组的形式返回请求体的内容
func (md *m3u8downloader) httpGetBodyToByte(url string) []byte {
	res, exception, err := httpGet(url)
	if exception != NoException {
		md.markException(exception, err)
		return nil
	}
	defer res.Close()
	body, err := io.ReadAll(res)
	if err != nil {
		md.markException(IOException, err)
		return nil
	}
	return body
}

func (md *m3u8downloader) DefaultDownload() bool {
	err := md.Download()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	err = md.MergeFile()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	return true
}

// SetUrl 设置需要下载视频的.m3u8文件的url
func (md *m3u8downloader) SetUrl(url string) {
	//设置url
	md.config.Url = []byte(url)
	//查找url中的最后一个‘/’,并作为noSuffixUrl的相关参数来构造noSuffixUrl
	last := reFind(len(md.config.Url)-1, md.config.Url)
	md.config.noSuffixUrl = make([]byte, last)
	md.config.noSuffixUrl = md.config.Url[0 : last+1]
}

// SetIfShowTheBar 是否显示进度条
func (md *m3u8downloader) SetIfShowTheBar(ifShow bool) {
	md.config.ifShowBar = ifShow
}

// SetNumOfThread 设置线程数量
func (md *m3u8downloader) SetNumOfThread(num int) {
	md.config.NumOfThreads = num
	md.buffer = make([]strings.Builder, num)

}

// SetMovieName 设置保存后的视频名称
func (md *m3u8downloader) SetMovieName(videoName string) {
	if strings.HasSuffix(videoName, SuffixTs) {
		md.config.VideoName = videoName
	} else {
		md.config.VideoName = videoName + SuffixTs
	}
}

// SetSaveDirectory 设置下载的视频的保存路径
func (md *m3u8downloader) SetSaveDirectory(targetDir string) {
	// 使用 filepath 库确保路径格式正确
	temp := targetDir
	if !strings.HasSuffix(temp, string(filepath.Separator)) {
		temp += string(filepath.Separator)
	}
	md.config.SaveDirectory = temp
}

// showTheBar 显示进度条方法
func (md *m3u8downloader) showTheBar() {
	md.printInfo()
	var total = md.config.TotalNum
	var bar = NewBar(int64(total))
	bar.Setting().SetShowModel(LinuxTerminal)
	var ok bool
	var num int
	bar.Update(0)
	for {
		num, ok = <-md.successChan
		if !ok {
			break
		}
		bar.Update(int64(num))
	}
	bar.Finish()

}

// reFind 倒序查找
func reFind(startIndex int, str []byte) int {
	var i int
	for i = startIndex; str[i] != '/'; i-- {
	}
	return i
}

// printInfo 打印下载信息
func (md *m3u8downloader) printInfo() {
	fmt.Printf("下载任务总数：%d\n", md.config.TotalNum)
	fmt.Printf("下载线程数量：%d\n", md.config.NumOfThreads)
	fmt.Printf("视频保存目录：%s\n", md.config.SaveDirectory)
	fmt.Printf("视频保存名称：%s\n", md.config.VideoName)
	fmt.Printf("视频下载链接：%s\n", md.config.Url)
}

// Download 下载任务核心方法
func (md *m3u8downloader) Download() error {
	var err error
	err = CheckAndCreatDirectory(md.config.SaveDirectory)
	if err != nil {
		return err
	}
	// 首先解析url
	md.m3u8ParseResult, err = md.ParseM3u8FileEncrypted(string(md.config.Url))
	if err != nil {
		return fmt.Errorf("解析 m3u8 失败: %w", err)
	}
	if md.config.VideoName == "" {
		//如果没有设置name,使用时间戳来代替
		md.config.VideoName = getUnixTimeAndToByte()
	}
	//将参数归零
	md.successChan = make(IntChannel, md.config.NumOfThreads)
	md.config.errCount = 0
	md.config.completeCount = 0
	md.exception = NoException
	md.lastErr = nil
	md.config.TotalNum = len(md.m3u8ParseResult.M3u8.Segments)
	md.suffixList = make([]string, md.config.TotalNum)
	md.waitGroup.Add(md.config.NumOfThreads)
	md.taskChannel = make(IntChannel, 50)
	//发布下载任务
	go md.publishDownloadTask()
	//开启下载线程（单一模式：下载为 ts 文件后合并）
	for i := 0; i < md.config.NumOfThreads; i++ {
		go md.download(i, md.SaveAsTsFileAndMergeEncryption)
	}
	//显示进度条
	if md.config.ifShowBar {
		go md.showTheBar()
	}
	//阻塞等待
	md.waitGroup.Wait()
	// 无论成功失败都关闭 successChan，避免进度条 goroutine 泄漏
	close(md.successChan)

	// 仅当错误累计达到致命阈值时判定为失败；
	// 普通的可重试错误若最终重试成功（errCount 未达阈值）则视为成功。
	md.config.errMutex.Lock()
	exc := md.exception
	lastErr := md.lastErr
	reachedFatal := md.config.errCount >= 2*md.config.NumOfThreads
	md.config.errMutex.Unlock()

	if reachedFatal {
		if lastErr != nil {
			return fmt.Errorf("%w: %v", errorMap[exc], lastErr)
		}
		return errorMap[exc]
	}
	return nil
}

// download 执行任务的主逻辑
func (md *m3u8downloader) download(threadId int, handle downloadCallback) {
	var index int
	var ok bool
	var body []byte
	var segments *Segment
	for {
		//从管道接收信息
		index, ok = <-md.taskChannel
		if !ok {
			break
		}
		//拼接路径
		segments = md.m3u8ParseResult.M3u8.Segments[index]
		fullURL := ResolveURL(md.m3u8ParseResult.URL, segments.URI)
		//尝试下载，以 body 是否为空判断成功，错误累计达到阈值后停止
		for {
			body = md.httpGetBodyToByte(fullURL)
			if body != nil {
				break
			}
			md.config.errMutex.Lock()
			if md.config.errCount >= 2*md.config.NumOfThreads {
				md.config.errMutex.Unlock()
				md.waitGroup.Done()
				return
			}
			md.config.errCount++
			md.config.errMutex.Unlock()
		}
		// 解密 TS 数据
		if segments.Key != nil {
			key := md.m3u8ParseResult.Keys[segments.Key]
			if key != "" {
				var err error
				body, err = AES128Decrypt(body, []byte(key), []byte(segments.Key.IV))
				if err != nil {
					// 解密失败属于不可重试的严重错误，立即停止所有线程
					md.config.errMutex.Lock()
					md.exception = DecrytTSFailed
					md.lastErr = err
					md.config.errCount = 2 * md.config.NumOfThreads
					md.config.errMutex.Unlock()
					md.waitGroup.Done()
					return
				}
			}
		}
		//处理解密
		syncByte := uint8(71) //0x47
		bLen := len(body)
		for j := 0; j < bLen; j++ {
			if body[j] == syncByte {
				body = body[j:]
				break
			}
		}
		//执行下载回调函数
		handle(index, threadId, body)
	}
	md.waitGroup.Done()
}

// SaveAsTsFileAndMergeEncryption 下载为ts文件，最后手动合并
func (md *m3u8downloader) SaveAsTsFileAndMergeEncryption(index, threadId int, body []byte) {
	md.buffer[threadId].WriteString(md.config.SaveDirectory)
	md.buffer[threadId].Write(processNum(index))
	md.suffixList[index] = md.buffer[threadId].String()
	movie, err := os.OpenFile(md.suffixList[index], os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		md.config.errMutex.Lock()
		md.exception = IOException
		md.lastErr = err
		md.config.errCount += md.config.NumOfThreads
		md.config.errMutex.Unlock()
		return
	}
	movie.Write(body)
	movie.Close()
	md.buffer[threadId].Reset()
	count := atomic.AddInt64(&md.config.completeCount, 1)
	md.successChan <- int(count)
	body = nil
}

// publishDownloadTask 发布下载任务
func (md *m3u8downloader) publishDownloadTask() {
	for i := 0; i < md.config.TotalNum; i++ {
		md.taskChannel <- i
	}
	close(md.taskChannel)
}

// MergeFileInDir 将目标目录下的ts文件全部合并
func (md *m3u8downloader) MergeFileInDir(path string, saveName string) error {
	fileList, err := getAllNonDirectoryFile(path)
	if err != nil {
		return err
	}
	return mergeFile(path, fileList, saveName)
}

// MergeFile 在下载完毕后文件合并
func (md *m3u8downloader) MergeFile() error {
	if md.suffixList == nil {
		return nil
	}
	return mergeFile(md.config.SaveDirectory, md.suffixList, md.config.VideoName)
}

// ParseM3u8FileEncrypted 解析并处理加密的m3u8文件
func (md *m3u8downloader) ParseM3u8FileEncrypted(link string) (*Result, error) {
	var (
		err       error
		_url      *url.URL
		exception DownloadExceptionType
		body      io.ReadCloser
	)

	_url, err = url.Parse(link)
	if err != nil {
		return nil, err
	}
	body, exception, err = httpGet(_url.String())
	if exception != NoException {
		return nil, fmt.Errorf("%w: %v", errorMap[exception], err)
	}
	//noinspection GoUnhandledErrorResult
	defer body.Close()
	s := bufio.NewScanner(body)
	var lines []string
	for s.Scan() {
		lines = append(lines, s.Text())
	}

	//解析请求体内容，m3u8中的内容
	m3u8, err := parseLines(lines)
	if err != nil {
		return nil, err
	}
	// 若为 Master playlist，则再次请求获取 Media playlist
	if m3u8.MasterPlaylistURIs != nil {
		return md.ParseM3u8FileEncrypted(ResolveURL(_url, m3u8.MasterPlaylistURIs[0]))
	}
	if len(m3u8.Segments) == 0 {
		return nil, errorMap[NoM3u8SegmentException]
	}
	result := &Result{
		URL:  _url,
		M3u8: m3u8,
		Keys: make(map[*Key]string),
	}
	// 请求解密秘钥
	var keyByte []byte
	for _, seg := range m3u8.Segments {
		switch {
		case seg.Key == nil || seg.Key.Method == "" || seg.Key.Method == CryptMethodNONE:
			continue
		case seg.Key.Method == CryptMethodAES:
			// 如果已经请求过了，就不在请求
			if _, ok := result.Keys[seg.Key]; ok {
				continue
			}

			keyByte = md.httpGetBodyToByte(ResolveURL(_url, seg.Key.URI))
			if keyByte == nil {
				if md.lastErr != nil {
					return nil, fmt.Errorf("%w: %v", errorMap[md.exception], md.lastErr)
				}
				return nil, errorMap[md.exception]
			}
			result.Keys[seg.Key] = string(keyByte)
		default:
			return nil, fmt.Errorf("[UnexpectedException]unknown or unsupported cryption method: %s", seg.Key.Method)
		}
	}
	return result, nil
}
