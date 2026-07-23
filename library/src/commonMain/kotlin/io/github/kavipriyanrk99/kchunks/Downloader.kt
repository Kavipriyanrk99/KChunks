package io.github.kavipriyanrk99.kchunks

import io.github.kavipriyanrk99.kchunks.service.ChunkSchedulerService
import io.github.kavipriyanrk99.kchunks.service.FileService
import io.github.kavipriyanrk99.kchunks.service.HttpService
import io.github.kavipriyanrk99.kchunks.utils.HttpUtils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import okio.Path
import kotlin.math.ceil
import kotlin.time.Clock


class Downloader(private val url: String, private val dirPath: Path) {
    // header properties
    private var contentLength: Long? = null
    private var contentDisposition: String? = null
    var allowRangeRequests = false
    var etag: String? = null
    var lastModified: String? = null
    lateinit var fileName: String
        private set

    private val _chunks = MutableStateFlow<Map<Int, Chunk>>(emptyMap())
    val chunks = _chunks.asStateFlow()

    private var downloadJob: Job? = null

    companion object {
        val StateFlow<Map<Int, Chunk>>.anyChunkInStartedState
            get() = this.value
                .values
                .run { isNotEmpty() && any { it.state is DownloadState.Started } }

        val StateFlow<Map<Int, Chunk>>.anyChunkInDownloadingState
            get() = this.value
                .values
                .run { isNotEmpty() && any { it.state is DownloadState.Downloading } }

        val StateFlow<Map<Int, Chunk>>.anyChunkInRetryState
            get() = this.value
                .values
                .run { isNotEmpty() && any { it.state is DownloadState.Retry } }

        val StateFlow<Map<Int, Chunk>>.anyChunkInUnknownState
            get() = this.value
                .values
                .run { isNotEmpty() && any { it.state is DownloadState.Unknown } }

        val StateFlow<Map<Int, Chunk>>.areAllChunksDownloaded
            get() = this.value
                .values
                .run { isNotEmpty() && all { it.state is DownloadState.Done } }
    }


    suspend fun multipartDownload() = withContext(Dispatchers.IO) {
        initializeHeaderBasedProperties()
        require(allowRangeRequests) { "Server doesn't support range-request. Multi-part downloading is not possible" }
        checkNotNull(contentLength) { "Multi-part download requires Content-Length" }
        _chunks.value = prepareChunks(contentLength!!)
        FileService.cleanFiles(chunks.value.values.map { it.filePath })
        val scheduler = ChunkSchedulerService(url, ArrayDeque(chunks.value.values), chunks, updateChunkStateFlow)
        downloadJob = scheduler.schedule(this)
        downloadJob!!.join()
        if (chunks.areAllChunksDownloaded) {
            val sortedFileParts = chunks.value
                .values
                .sortedBy { it.id }
                .map { it.filePath }
            FileService.combineFiles(fileName, sortedFileParts)
        }
    }

    suspend fun cancel(): Boolean {
        checkNotNull(downloadJob) { "Download Job was not initialized. No download was started" }

        if (downloadJob!!.isCompleted && chunks.areAllChunksDownloaded)
            return false

        downloadJob!!.cancelAndJoin()
        FileService.cleanFiles(chunks.value.values.map { it.filePath })
        return true
    }

    suspend fun pause(): Boolean {
        checkNotNull(downloadJob) { "Download Job was not initialized. No download was started" }

        if (downloadJob!!.isCompleted && chunks.areAllChunksDownloaded)
            return false

        downloadJob!!.cancelAndJoin()
        return true
    }

    suspend fun resume() = withContext(Dispatchers.IO) {
        if (chunks.areAllChunksDownloaded)
            return@withContext

        if (downloadJob == null || chunks.value.isEmpty()) {
            initializeHeaderBasedProperties()
            require(allowRangeRequests) { "Server doesn't support range-request. Multi-part downloading is not possible" }
            checkNotNull(contentLength) { "Multi-part download requires Content-Length" }
            _chunks.value = prepareChunks(contentLength!!)
        }

        chunks.value.values.forEach { chunk ->
            val newOffset = chunk.computeUpdatedCurrentOffset()
            updateChunkStateFlow(chunk.id) {
                copy(
                    currentOffset = newOffset,
                    state = if (endByte == newOffset) DownloadState.Done else DownloadState.Unknown
                )
            }
        }
        val scheduler = ChunkSchedulerService(url, ArrayDeque(chunks.value.values), chunks, updateChunkStateFlow)
        downloadJob = scheduler.schedule(this)
        downloadJob!!.join()
        if (chunks.areAllChunksDownloaded) {
            val sortedFileParts = chunks.value
                .values
                .sortedBy { it.id }
                .map { it.filePath }
            FileService.combineFiles(fileName, sortedFileParts)
        }
    }

    private suspend fun initializeHeaderBasedProperties() {
        val headers = HttpService.getHeaders(url)

        this.contentLength = headers["Content-Length"]?.toLongOrNull()
        this.contentDisposition = headers["Content-Disposition"]
        headers["Accept-Ranges"]?.let {
            if (it == "bytes")
                this.allowRangeRequests = true
        }
        this.etag = headers["ETag"]
        this.lastModified = headers["Last-Modified"]
        prepareFileName()
    }

    private fun prepareFileName() {
        if (this::fileName.isInitialized)
            return

        var preparedFileName = ""
        contentDisposition?.let {
            preparedFileName = HttpUtils.prepareFileNameFromContentDisposition(it)
        }

        if (preparedFileName.isBlank())
            preparedFileName = HttpUtils.prepareFileNameFromURL(url)

        if (preparedFileName.isBlank())
            preparedFileName = "kchunks-" + Clock.System.now()

        this.fileName = preparedFileName
    }

    private fun prepareChunks(
        fileSize: Long,
        noOfChunks: Int = KChunksDefaults.DEFAULT_NO_OF_CHUNKS,
        minChunkSize: Long = KChunksDefaults.DEFAULT_CHUNK_SIZE
    ): Map<Int, Chunk> {
        var finalNoOfChunks = noOfChunks
        while (ceil(fileSize.toDouble() / finalNoOfChunks) < minChunkSize && finalNoOfChunks > 1)
            finalNoOfChunks -= 1

        val evenChunkSize = fileSize / finalNoOfChunks
        var start = 0L
        var end: Long
        val chunks = mutableMapOf<Int, Chunk>()
        for (i in 1..finalNoOfChunks) {
            val key = "chunk-$i"
            val fileName = "${fileName}.chunk-$i.tmp"
            if (i == finalNoOfChunks) {
                end = fileSize - 1
                chunks[i] = Chunk(
                    id = i,
                    startByte = start,
                    endByte = end,
                    currentOffset = start,
                    etag = etag ?: "",
                    filePath = dirPath.resolve(fileName)
                )
                continue
            }

            end = start + evenChunkSize
            chunks[i] = Chunk(
                id = i,
                startByte = start,
                endByte = end,
                currentOffset = start,
                etag = etag ?: "",
                filePath = dirPath.resolve(fileName)
            )
            start = end + 1
        }

        return chunks
    }

    private val updateChunkStateFlow = { chunkId: Int, transform: Chunk.() -> Chunk ->
        _chunks.update { oldChunks ->
            val oldChunk = oldChunks[chunkId]
            oldChunk?.let {
                oldChunks + (chunkId to it.transform())
            } ?: oldChunks
        }
    }
}