import DataSize.Companion.bytes
import DataSize.Companion.kibibytes
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
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
    private var contentDisposition: String? = null
    lateinit var fileName: String

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

    private suspend fun streamingDownload() = httpClient.prepareGet(url).execute { response ->
        if (response.isSuccessful().not()) {
            return@execute
        }

        val contentLength = response.contentLength()?.bytes
        this@KChunks.contentDisposition = response.headers["Content-Disposition"]
        prepareFileName()

        _downloadState.value = DownloadState.Started
        val channel: ByteReadChannel = response.body()
        var readBytes = 0L
        var prevReadBytes = 0L

        val filePath = path.resolve(fileName)
        FileSystem.SYSTEM.write(filePath) {
            while (!channel.exhausted()) {
                lateinit var chunk: Source
                val duration = measureTime {
                    chunk = channel.readRemaining(bufferSize.bytes)
                    prevReadBytes = readBytes
                    readBytes += chunk.remaining
                    this.write(chunk.buffered().readByteArray())
                }

                if(downloadSamples.size == 10) {
                    downloadSamples.removeFirst()
                }
                downloadSamples.add(DownloadSample((readBytes - prevReadBytes).bytes, duration))

                val downloadSpeed = DataUtils.calculateDownloadSpeed(downloadSamples)
                val downloadETA = if(contentLength == null) Double.POSITIVE_INFINITY
                    else DataUtils.calculateDownloadETA(contentLength!!, readBytes.bytes, downloadSpeed)
                val downloadPercentage = if(contentLength == null) Double.NaN
                    else DataUtils.calculateDownloadPercentage(contentLength!!, readBytes.bytes)

                _downloadState.value = DownloadState.Downloading(
                    speed = downloadSpeed,
                    eta = downloadETA,
                    percentage = downloadPercentage
                )

                println("Received $readBytes bytes from ${contentLength?.bytes ?: "UNKNOWN"} | Progress: $downloadSpeed bytes/sec, $downloadETA ETA in sec, ${downloadPercentage}%")
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

    suspend fun cancel() {
        _downloadState.value = DownloadState.Failed
        if(job != null && job!!.isActive)
            job!!.cancelAndJoin()
        closeHttpClient()
    }
}