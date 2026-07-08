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
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.io.Source
import kotlinx.io.buffered
import kotlinx.io.readByteArray
import okio.FileSystem
import okio.Path
import okio.SYSTEM
import okio.buffer
import okio.use
import kotlin.coroutines.cancellation.CancellationException
import kotlin.let
import kotlin.math.log
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

class KChunks(private val url: String, private val path: Path) {
    private val httpClient by lazy {
        HttpClient {
            install(HttpTimeout) {
                requestTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
                connectTimeoutMillis = 50_000
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

    private lateinit var chunks: MutableMap<String, Pair<Long, Long>>
    private val tempChunks: MutableMap<String, Pair<Long, Long>> = mutableMapOf()
    private val jobCoroutineNameMap: MutableMap<Job, String> = mutableMapOf()

    private val _downloadStateMultipart = MutableStateFlow<MutableMap<Job, DownloadState>>(mutableMapOf())
    val downloadStateMultipart = _downloadStateMultipart.asStateFlow()

    private fun closeHttpClient() = httpClient.close()

    private fun HttpResponse.isSuccessful() = when {
        this.status.isSuccess() -> true
        else -> {
            println("Request failed with status code: ${this.status.value}, msg: ${this.status.description}")
            false
        }
    }

    private fun prepareFileName() {
        if (this::fileName.isInitialized)
            return

        var preparedFileName = ""
        contentDisposition?.let {
            preparedFileName = HttpUtils.prepareFileNameFromURL(it)
        }

        if (preparedFileName.isBlank())
            preparedFileName = HttpUtils.prepareFileNameFromURL(url)

        if (preparedFileName.isBlank())
            preparedFileName = IOUtils.prepareDefaultFileName()

        this.fileName = preparedFileName
    }

    private suspend fun initializeHeaderBasedProperties() {
        val headers = httpClient.prepareGet(url).execute {
            it.headers
        }

        this.contentLength = headers["Content-Length"]?.toLongOrNull()?.bytes
        this.contentDisposition = headers["Content-Disposition"]
        headers["Accept-Ranges"]?.let {
            if (it == "bytes")
                this.allowRangeRequests = true
        }
        this.etag = headers["ETag"]
        this.lastModified = headers["Last-Modified"]
        prepareFileName()
        IOUtils.log("Content-Length: ${this.contentLength}, Content-Disposition: ${this.contentDisposition}, " +
                "Accept-Ranges: ${this.allowRangeRequests}, Etag: ${this.etag}, Last-Modified: ${this.lastModified}, filename: ${this.fileName}")
    }

    private suspend fun streamingDownload(customHeaders: Headers = Headers.Empty) = httpClient.prepareGet(url) {
        if (customHeaders !== Headers.Empty)
            this.headers.appendAll(customHeaders)
    }.execute { response ->
        if (response.isSuccessful().not()) {
            return@execute
        }

        initializeHeaderBasedProperties()
        prepareFileName()

        _downloadState.value = DownloadState.Started
        val channel: ByteReadChannel = response.body()
        var readBytes = 0L
        var prevReadBytes = 0L

        val filePath = path.resolve(fileName)
        val bufferedTargetFileSink = if (response.status == HttpStatusCode.PartialContent) {
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

                if (downloadSamples.size == 10) {
                    downloadSamples.removeFirst()
                }
                downloadSamples.add(DownloadSample((readBytes - prevReadBytes).bytes, duration))

                val downloadSpeed = DataUtils.calculateDownloadSpeed(downloadSamples)
                val downloadETA = if (this@KChunks.contentLength == null) Double.POSITIVE_INFINITY
                else DataUtils.calculateDownloadETA(this@KChunks.contentLength!!, readBytes.bytes, downloadSpeed)
                val downloadPercentage = if (this@KChunks.contentLength == null) Double.NaN
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
        if (this.allowRangeRequests.not())
            error("Unsupported operation 'pause' as range-requests is not allowed for the download")

        _downloadState.value = DownloadState.Paused
        if (job != null && job!!.isActive)
            job!!.cancelAndJoin()
//        closeHttpClient()
    }

    suspend fun resume(fileName: String = "", etagOrLastModified: String) = coroutineScope {
        require(etagOrLastModified.isNotBlank()) { "Download resuming failed. Resuming a download requires Etag or Last-Modified header value for resource integrity" }

        if (fileName.isNotBlank())
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

    private suspend fun awaitChunksInDownloading() = _downloadStateMultipart.first { map ->
        map.isNotEmpty() && map.values.all { it is DownloadState.Downloading }
    }


    private fun calculateCurrentThroughput(): Double = _downloadStateMultipart.value.values.sumOf {
        (it as? DownloadState.Downloading)?.percentage ?: 0.0
    }

    private suspend fun CoroutineScope.launchWorkers(count: Int = 1) = repeat(count) {
        val coroutineName = chunks.keys.firstOrNull()
        chunks.remove(coroutineName)?.let { bytePair ->
            tempChunks[coroutineName!!] = bytePair
            val range = "bytes=${bytePair.first}-${bytePair.second}"
            val customHeaders = Headers.build {
                append("If-Match", this@KChunks.etag ?: "")
                append("Range", range)
            }

            launch(CoroutineName(coroutineName)) {
                streamingDownloadMultipart(customHeaders)
            }.also { job ->
                _downloadStateMultipart.update { old ->
                    old.toMutableMap().apply {
                        this[job] = DownloadState.Unknown
                    }
                }
                jobCoroutineNameMap[job] = coroutineName
            }
        }
    }

    private fun killSlowWorkers(count: Int) = repeat(count) {
        val slowState = _downloadStateMultipart.value.minByOrNull {
            (it.value as? DownloadState.Downloading)?.percentage ?: 0.0
        }

        if (slowState != null) {
            val coroutineName = jobCoroutineNameMap[slowState.key]
            slowState.key.cancel()
            _downloadStateMultipart.update { old ->
                old.toMutableMap().apply {
                    remove(slowState.key)
                }
            }
            tempChunks.remove(coroutineName)?.let {
                chunks[coroutineName!!] = it
            }
            IOUtils.log("Killing worker of chunk: $coroutineName")
        }
    }

    suspend fun CoroutineScope.streamingDownloadMultipart(customHeaders: Headers = Headers.Empty) {
        val chunkCoroutineJob = coroutineContext[Job]
        val chunkCoroutineName = jobCoroutineNameMap[chunkCoroutineJob]
        IOUtils.log(coroutineContext, "streamingDownloadMultipart started for chunk: $chunkCoroutineName")

        try {
            httpClient.prepareGet(url) {
                if (customHeaders !== Headers.Empty)
                    this.headers.appendAll(customHeaders)
            }.execute { response ->
                if (response.isSuccessful().not() || response.status != HttpStatusCode.PartialContent) {
                    IOUtils.log(coroutineContext, "Range-request failed chunk: $chunkCoroutineName")
                    error("Range-request failed")
                }

                val chunkFileName = "${this@KChunks.fileName}.${chunkCoroutineName}"
                val chunkContentLength = response.headers["Content-Length"]?.toLongOrNull()?.bytes
                val chunkDownloadSample = ArrayDeque<DownloadSample>()

                _downloadStateMultipart.update { old ->
                    old.toMutableMap().apply {
                        this[chunkCoroutineJob!!] = DownloadState.Started
                    }
                }
                val channel: ByteReadChannel = response.body()
                var readBytes = 0L
                var prevReadBytes = 0L

                val chunkFilePath = path.resolve(chunkFileName)
                FileSystem.SYSTEM.write(chunkFilePath) {
                    while (!channel.exhausted()) {
                        lateinit var chunk: Source
                        val duration = measureTime {
                            chunk = channel.readRemaining(bufferSize.bytes)
                            prevReadBytes = readBytes
                            readBytes += chunk.remaining
                            this.write(chunk.buffered().readByteArray())
                        }

                        if (chunkDownloadSample.size == 10) {
                            chunkDownloadSample.removeFirst()
                        }
                        chunkDownloadSample.add(DownloadSample((readBytes - prevReadBytes).bytes, duration))

                        val downloadSpeed = DataUtils.calculateDownloadSpeed(chunkDownloadSample)
                        val downloadETA = if (chunkContentLength == null) Double.POSITIVE_INFINITY
                        else DataUtils.calculateDownloadETA(chunkContentLength, readBytes.bytes, downloadSpeed)
                        val downloadPercentage = if (chunkContentLength == null) Double.NaN
                        else DataUtils.calculateDownloadPercentage(chunkContentLength, readBytes.bytes)

                        _downloadStateMultipart.update { old ->
                            old.toMutableMap().apply {
                                this[chunkCoroutineJob!!] = DownloadState.Downloading(
                                    speed = downloadSpeed,
                                    eta = downloadETA,
                                    percentage = downloadPercentage
                                )
                            }
                        }

                        IOUtils.log(coroutineContext, "Chunk: $chunkCoroutineName, Received $readBytes bytes from ${chunkContentLength?.bytes ?: "UNKNOWN"} | Progress: $downloadSpeed bytes/sec, $downloadETA ETA in sec, ${downloadPercentage}%")
                    }
                }
            }
        } catch (ce: CancellationException) {
            IOUtils.log(coroutineContext, ce)
            throw ce
        } catch (e: Exception) {
            IOUtils.log(coroutineContext, e)
            tempChunks.remove(chunkCoroutineName)?.let {
                chunks[chunkCoroutineName!!] = it
            }
            throw e
        } finally {
            _downloadStateMultipart.update { old ->
                old.toMutableMap().apply {
                    remove(chunkCoroutineJob)
                }
            }
        }
    }

    suspend fun multipartDownload(initialWorkers: Int = 4) = withContext(Dispatchers.IO) {
        initializeHeaderBasedProperties()
        require(allowRangeRequests) { "Server doesn't support range-request. Multi-part downloading is not possible" }
        checkNotNull(contentLength) { "Multi-part download requires Content-Length" }
        this@KChunks.chunks = HttpUtils.prepareChunks(contentLength!!)

        var currentWorkers = initialWorkers
        val totalChunks = chunks.size
        var optimalWorkers = initialWorkers
        launchWorkers(currentWorkers)

        launch {
            // waits till chunks in downloadState goes to DownloadState.Download
            awaitChunksInDownloading()
            var prevThroughput = calculateCurrentThroughput()
            var currentThroughput = 0.0
            while (tempChunks.size != totalChunks) {
                delay(10.seconds)
                currentThroughput = calculateCurrentThroughput()
                currentWorkers = _downloadStateMultipart.value.size
                if(currentWorkers == 0)
                    launchWorkers(optimalWorkers)
                else {
                    if (currentThroughput - prevThroughput >= 15) {
                        launchWorkers()
                    } else {
                        currentWorkers /= 2
                        killSlowWorkers(currentWorkers)
                    }

                    currentWorkers = _downloadStateMultipart.value.size
                    if(currentWorkers != 0)
                        optimalWorkers = currentWorkers
                }

                prevThroughput = currentThroughput
                IOUtils.log("Optimal workers: $optimalWorkers, current workers: $currentWorkers")
            }
        }
    }

    suspend fun cancel() {
        _downloadState.value = DownloadState.Failed
        if (job != null && job!!.isActive)
            job!!.cancelAndJoin()
        closeHttpClient()
    }
}