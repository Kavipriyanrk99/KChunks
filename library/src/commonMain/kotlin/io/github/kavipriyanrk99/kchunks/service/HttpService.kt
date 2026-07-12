package io.github.kavipriyanrk99.kchunks.service

import DataSize.Companion.bytes
import DataUtils
import DownloadSample
import IOUtils
import io.github.kavipriyanrk99.kchunks.Chunk
import io.github.kavipriyanrk99.kchunks.DownloadState
import io.github.kavipriyanrk99.kchunks.KChunksDefaults
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.utils.io.*
import io.ktor.utils.io.core.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.io.Source
import kotlinx.io.buffered
import kotlinx.io.readByteArray
import okio.FileSystem
import okio.Path
import okio.SYSTEM
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.measureTime

internal object HttpService {
    private fun HttpResponse.isSuccessful() = when {
        this.status.isSuccess() -> true
        else -> {
            println("Request failed with status code: ${this.status.value}, msg: ${this.status.description}")
            false
        }
    }

    suspend fun getHeaders(url: String) = KChunksDefaults.defaultHttpClient.prepareGet(url).execute {
        it.headers
    }

    suspend fun CoroutineScope.streamingDownload(
        url: String,
        chunkName: String,
        chunkFilePath: Path,
        customHeaders: Headers = Headers.Empty,
        bufferSize: Long = KChunksDefaults.DEFAULT_BUFFER_SIZE,
        updateChunkStateFlow: (chunkName: String, transform: Chunk.() -> Chunk) -> Unit
    ) {
//        val chunkCoroutineJob = coroutineContext[Job]
//        val chunkCoroutineName = jobCoroutineNameMap[chunkCoroutineJob]
        IOUtils.log(coroutineContext, "streamingDownloadMultipart started for chunk: $chunkName")

        try {
            KChunksDefaults.defaultHttpClient.prepareGet(url) {
                if (customHeaders !== Headers.Empty)
                    this.headers.appendAll(customHeaders)
            }.execute { response ->
                if (response.isSuccessful().not() || response.status != HttpStatusCode.PartialContent) {
                    IOUtils.log(coroutineContext, "Range-request failed chunk: $chunkName")
                    error("Range-request failed")
                }

//                val chunkFileName = "${this@KChunks.fileName}.${chunkCoroutineName}"
                val chunkContentLength = response.headers["Content-Length"]?.toLongOrNull()?.bytes
                val chunkDownloadSample = ArrayDeque<DownloadSample>()

//                _downloadStateMultipart.update { old ->
//                    old.toMutableMap().apply {
//                        this[chunkCoroutineJob!!] = DownloadState.Started
//                    }
//                }
//                _chunks.update { oldChunks ->
//                    oldChunks + (chunkName to oldChunks[chunkName]!!.copy(state = DownloadState.Downloading))
//                }
                updateChunkStateFlow(chunkName) {
                    copy(state = DownloadState.Downloading)
                }
                val channel: ByteReadChannel = response.body()
                var readBytes = 0L
                var prevReadBytes = 0L

//                val chunkFilePath = path.resolve(chunkFileName)
                FileSystem.SYSTEM.write(chunkFilePath) {
                    while (!channel.exhausted()) {
                        lateinit var chunk: Source
                        val duration = measureTime {
                            chunk = channel.readRemaining(bufferSize)
                            prevReadBytes = readBytes
                            readBytes += chunk.remaining
                            this.write(chunk.buffered().readByteArray())
                        }

                        if (chunkDownloadSample.size == KChunksDefaults.DEFAULT_SAMPLE_SIZE) {
                            chunkDownloadSample.removeFirst()
                        }
                        chunkDownloadSample.add(DownloadSample((readBytes - prevReadBytes).bytes, duration))

                        val downloadSpeed = DataUtils.calculateDownloadSpeed(chunkDownloadSample)
                        val downloadETA = if (chunkContentLength == null) Double.POSITIVE_INFINITY
                        else DataUtils.calculateDownloadETA(chunkContentLength, readBytes.bytes, downloadSpeed)
                        val downloadPercentage = if (chunkContentLength == null) Double.NaN
                        else DataUtils.calculateDownloadPercentage(chunkContentLength, readBytes.bytes)

//                        _downloadStateMultipart.update { old ->
//                            old.toMutableMap().apply {
//                                this[chunkCoroutineJob!!] = DownloadState.Downloading(
//                                    speed = downloadSpeed,
//                                    eta = downloadETA,
//                                    percentage = downloadPercentage
//                                )
//                            }
//                        }
//                        _chunks.update { oldChunks ->
//                            oldChunks + (chunkName to oldChunks[chunkName]!!.copy(
//                                speed = downloadSpeed,
//                                eta = downloadETA,
//                                percentage = downloadPercentage
//                            ))
//                        }
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
            IOUtils.log(coroutineContext, ce)
            throw ce
        } catch (e: Exception) {
            IOUtils.log(coroutineContext, e)
//            tempChunks.remove(chunkCoroutineName)?.let {
//                chunks[chunkCoroutineName!!] = it
//            }
            throw e
        } finally {
//            _downloadStateMultipart.update { old ->
//                old.toMutableMap().apply {
//                    remove(chunkCoroutineJob)
//                }
//            }
        }
    }
}