package io.github.kavipriyanrk99.kchunks.service

import DataSize.Companion.bytes
import DataUtils
import DownloadSample
import IOUtils
import io.github.kavipriyanrk99.kchunks.Chunk
import io.github.kavipriyanrk99.kchunks.DefaultEpochMetrics
import io.github.kavipriyanrk99.kchunks.DownloadState
import io.github.kavipriyanrk99.kchunks.EpochMetrics
import io.github.kavipriyanrk99.kchunks.KChunksDefaults
import io.github.kavipriyanrk99.kchunks.NoOpEpochMetrics
import io.github.kavipriyanrk99.kchunks.NonRetryableNetworkException
import io.github.kavipriyanrk99.kchunks.RetryableNetworkException
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.utils.io.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.io.Source
import kotlinx.io.buffered
import kotlinx.io.readByteArray
import okio.FileSystem
import okio.Path
import okio.SYSTEM
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.measureTime

internal object HttpService {
    private val RETRYABLE_CODES = setOf(
        408,
        421,
        425,
        429,
        500,
        502,
        503,
        504,
        507
    )

    private fun HttpResponse.isSuccessful() = when {
        this.status.isSuccess() -> true
        else -> {
            println("Request failed with status code: ${this.status.value}, msg: ${this.status.description}")
            false
        }
    }

    private fun HttpResponse.validate(customHeaders: Headers, rangeRequest: Boolean = false): Unit = when {
        rangeRequest && this.status != HttpStatusCode.PartialContent ->
            throw NonRetryableNetworkException("Range request failed with status code: ${this.status.value}, msg: ${this.status.description}")

        isSuccessful() -> return

        this.status.value in RETRYABLE_CODES ->
            throw RetryableNetworkException(
                "Request failed with status code: ${this.status.value}, msg: ${this.status.description}",
                retryAfter = headers["Retry-After"]
            )

        this.status == HttpStatusCode.PreconditionFailed ->
            throw NonRetryableNetworkException("Conditional request failed with status code: ${this.status.value}, msg: ${this.status.description}")

        status == HttpStatusCode.RequestedRangeNotSatisfiable ->
            throw NonRetryableNetworkException("Invalid Range header: ${customHeaders["Range"]}. Request failed with status code: ${this.status.value}, msg: ${this.status.description}")

        else -> throw NonRetryableNetworkException("Request failed with status code: ${this.status.value}, msg: ${this.status.description}")
    }

    suspend fun getHeaders(url: String) = KChunksDefaults.defaultHttpClient.prepareGet(url).execute {
        it.headers
    }

    suspend fun CoroutineScope.streamingDownload(
        url: String,
        chunkName: String,
        chunkFilePath: Path,
        epochMetrics: EpochMetrics = NoOpEpochMetrics,
        rangeRequest: Boolean = false,
        customHeaders: Headers = Headers.Empty,
        bufferSize: Long = KChunksDefaults.DEFAULT_BUFFER_SIZE,
        updateChunkStateFlow: (chunkName: String, transform: Chunk.() -> Chunk) -> Unit
    ) {
        IOUtils.log(coroutineContext, "streamingDownloadMultipart started for chunk: $chunkName")

        try {
            KChunksDefaults.defaultHttpClient.prepareGet(url) {
                if (customHeaders !== Headers.Empty)
                    this.headers.appendAll(customHeaders)
            }.execute { response ->
                response.validate(customHeaders, rangeRequest)

                val chunkContentLength = response.headers["Content-Length"]?.toLongOrNull()?.bytes
                val chunkDownloadSample = ArrayDeque<DownloadSample>()
                val channel: ByteReadChannel = response.body()
                var readBytes = 0L
                var prevReadBytes = 0L

                updateChunkStateFlow(chunkName) {
                    copy(state = DownloadState.Downloading)
                }

                FileSystem.SYSTEM.write(chunkFilePath) {
                    while (!channel.exhausted()) {
                        lateinit var chunk: Source
                        var chunkReadLatency: Duration
                        val duration = measureTime {
                            chunkReadLatency = measureTime { chunk = channel.readRemaining(bufferSize) }
                            prevReadBytes = readBytes
                            readBytes += chunk.remaining
                            this.write(chunk.buffered().readByteArray())
                        }

                        if (epochMetrics is DefaultEpochMetrics)
                            epochMetrics.record(chunkReadLatency)

                        if (chunkDownloadSample.size == KChunksDefaults.DEFAULT_SAMPLE_SIZE) {
                            chunkDownloadSample.removeFirst()
                        }
                        chunkDownloadSample.add(DownloadSample((readBytes - prevReadBytes).bytes, duration))

                        val downloadSpeed = DataUtils.calculateDownloadSpeed(chunkDownloadSample)
                        val downloadETA = if (chunkContentLength == null) Double.POSITIVE_INFINITY
                        else DataUtils.calculateDownloadETA(chunkContentLength, readBytes.bytes, downloadSpeed)
                        val downloadPercentage = if (chunkContentLength == null) Double.NaN
                        else DataUtils.calculateDownloadPercentage(chunkContentLength, readBytes.bytes)

                        updateChunkStateFlow(chunkName) {
                            copy(
                                speed = downloadSpeed,
                                eta = downloadETA,
                                percentage = downloadPercentage
                            )
                        }

                        IOUtils.log(
                            coroutineContext,
                            "Chunk: $chunkName, Received $readBytes bytes from ${chunkContentLength?.bytes ?: "UNKNOWN"} | Progress: $downloadSpeed bytes/sec, $downloadETA ETA in sec, ${downloadPercentage}%"
                        )
                    }
                }
            }
        } catch (ce: CancellationException) {
            throw ce
        } catch (e: Exception) {
            throw e
        }
    }
}