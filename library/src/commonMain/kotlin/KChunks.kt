import DataSize.Companion.bytes
import DataSize.Companion.kibibytes
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.util.reflect.instanceOf
import io.ktor.utils.io.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.io.Source
import kotlinx.io.buffered
import kotlinx.io.readByteArray
import okio.FileSystem
import okio.Path
import okio.SYSTEM
import okio.buffer
import okio.use
import kotlin.time.measureTime

class KChunks(private val url: String, private val path: Path) {
    private val httpClient by lazy {
        HttpClient {
            install(HttpTimeout) {
                requestTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
                connectTimeoutMillis = 10_000
                socketTimeoutMillis = 30_000
            }
        }
    }
    private val bufferSize = 16L.kibibytes
    private var job: Job? = null
    private var contentLength: DataSize? = null
    private var contentDisposition: String? = null
    var allowRangeRequests = false
    var etag: String? = null
    var lastModified: String? = null
    lateinit var fileName: String
        private set

    private val _downloadState = MutableStateFlow<DownloadState>(DownloadState.Unknown)
    val downloadState = _downloadState.asStateFlow()

    private val downloadSamples = ArrayDeque<DownloadSample>(10)

    private fun closeHttpClient() = httpClient.close()

    private fun HttpResponse.isSuccessful() = when {
        this.status.isSuccess() -> true
        else -> {
            println("Request failed with status code: ${this.status.value}, msg: ${this.status.description}")
            false
        }
    }

    private fun prepareFileName() {
        if(this::fileName.isInitialized)
            return

        var preparedFileName = ""
        contentDisposition?.let {
            preparedFileName = HttpUtils.prepareFileNameFromURL(it)
        }

        if(preparedFileName.isBlank())
            preparedFileName = HttpUtils.prepareFileNameFromURL(url)

        if(preparedFileName.isBlank())
            preparedFileName = IOUtils.prepareDefaultFileName()

        this.fileName = preparedFileName
    }

    private fun prepareHeaderBasedProperties(headers: Headers) {
        this.contentLength = headers["Content-Length"]?.toLongOrNull()?.bytes
        this.contentDisposition = headers["Content-Disposition"]
        headers["Accept-Ranges"]?.let {
            if(it == "bytes")
                this.allowRangeRequests = true
        }
        this.etag = headers["ETag"]
        this.lastModified = headers["Last-Modified"]
    }

    private suspend fun streamingDownload(customHeaders: Headers = Headers.Empty) = httpClient.prepareGet(url) {
        if(customHeaders !== Headers.Empty)
            this.headers.appendAll(customHeaders)
    }.execute { response ->
        if (response.isSuccessful().not()) {
            return@execute
        }

        prepareHeaderBasedProperties(response.headers)
        prepareFileName()

        _downloadState.value = DownloadState.Started
        val channel: ByteReadChannel = response.body()
        var readBytes = 0L
        var prevReadBytes = 0L

        val filePath = path.resolve(fileName)
        val bufferedTargetFileSink = if(response.status == HttpStatusCode.PartialContent) {
            FileSystem.SYSTEM.appendingSink(filePath, true).buffer()
        } else {
            FileSystem.SYSTEM.sink(filePath).buffer()
        }

        bufferedTargetFileSink.use {
            while (!channel.exhausted()) {
                lateinit var chunk: Source
                val duration = measureTime {
                    chunk = channel.readRemaining(bufferSize.bytes)
                    prevReadBytes = readBytes
                    readBytes += chunk.remaining
                    it.write(chunk.buffered().readByteArray())
                }

                if(downloadSamples.size == 10) {
                    downloadSamples.removeFirst()
                }
                downloadSamples.add(DownloadSample((readBytes - prevReadBytes).bytes, duration))

                val downloadSpeed = DataUtils.calculateDownloadSpeed(downloadSamples)
                val downloadETA = if(this@KChunks.contentLength == null) Double.POSITIVE_INFINITY
                    else DataUtils.calculateDownloadETA(this@KChunks.contentLength!!, readBytes.bytes, downloadSpeed)
                val downloadPercentage = if(this@KChunks.contentLength == null) Double.NaN
                    else DataUtils.calculateDownloadPercentage(this@KChunks.contentLength!!, readBytes.bytes)

                _downloadState.value = DownloadState.Downloading(
                    speed = downloadSpeed,
                    eta = downloadETA,
                    percentage = downloadPercentage
                )

                println("Received $readBytes bytes from ${this@KChunks.contentLength?.bytes ?: "UNKNOWN"} | Progress: $downloadSpeed bytes/sec, $downloadETA ETA in sec, ${downloadPercentage}%")
            }
        }

        _downloadState.value = DownloadState.Done
    }

    suspend fun download() = coroutineScope {
        job = launch {
            streamingDownload()
            closeHttpClient()
        }
    }

    suspend fun download(fileName: String) = coroutineScope {
        require(fileName.isNotBlank()) { "Custom filename can't be empty" }
        this@KChunks.fileName = fileName

        job = launch {
            streamingDownload()
            closeHttpClient()
        }
    }

    suspend fun pause() {
        if(this.allowRangeRequests.not())
            error("Unsupported operation 'pause' as range-requests is not allowed for the download")

        _downloadState.value = DownloadState.Paused
        if(job != null && job!!.isActive)
            job!!.cancelAndJoin()
//        closeHttpClient()
    }

    suspend fun resume(fileName: String = "", etagOrLastModified: String) = coroutineScope {
        require(etagOrLastModified.isNotBlank()) { "Download resuming failed. Resuming a download requires Etag or Last-Modified header value for resource integrity" }

        if(fileName.isNotBlank())
            this@KChunks.fileName = fileName

        require(this@KChunks::fileName.isInitialized) { "No filename found to resume download" }

        val filePath = path.resolve(this@KChunks.fileName)
        val fileSizeInBytes = IOUtils.findFileSizeInBytes(filePath)
        checkNotNull(fileSizeInBytes) { "Download resuming failed. Unable to determine the filesize of ${filePath.name}" }

        val range = "bytes=${fileSizeInBytes}-"
        val rangeRequestHeaders = Headers.build {
            append("If-Match", etagOrLastModified)
            append("Range", range)
        }

        job = launch {
            streamingDownload(rangeRequestHeaders)
            closeHttpClient()
        }
    }

    suspend fun cancel() {
        _downloadState.value = DownloadState.Failed
        if(job != null && job!!.isActive)
            job!!.cancelAndJoin()
        closeHttpClient()
    }
}