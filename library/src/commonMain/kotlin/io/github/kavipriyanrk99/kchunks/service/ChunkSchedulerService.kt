package io.github.kavipriyanrk99.kchunks.service

import IOUtils
import io.github.kavipriyanrk99.kchunks.*
import io.github.kavipriyanrk99.kchunks.Downloader.Companion.anyChunkInDownloadingState
import io.github.kavipriyanrk99.kchunks.Downloader.Companion.anyChunkInRetryState
import io.github.kavipriyanrk99.kchunks.Downloader.Companion.anyChunkInStartedState
import io.github.kavipriyanrk99.kchunks.Downloader.Companion.anyChunkInUnknownState
import io.github.kavipriyanrk99.kchunks.utils.AdjustableSemaphore
import io.ktor.http.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.max
import kotlin.math.min
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

class ChunkSchedulerService(
    private val url: String,
    private val chunkQueue: ArrayDeque<Chunk>,
    private val chunks: StateFlow<Map<Int, Chunk>>,
    val updateChunkStateFlow: (chunkId: Int, transform: Chunk.() -> Chunk) -> Unit
) {
    private companion object {
        const val MIN_LIMIT = KChunksDefaults.CONCURRENCY_LIMIT_MIN_LIMIT
        const val INITIAL_LIMIT = KChunksDefaults.CONCURRENCY_LIMIT_INITIAL_LIMIT
        const val MAX_LIMIT = KChunksDefaults.CONCURRENCY_LIMIT_MAX_LIMIT
        const val BACKOFF_RATIO = KChunksDefaults.CONCURRENCY_LIMIT_BACKOFF_RATIO
        const val TIMEOUT = KChunksDefaults.CONCURRENCY_LIMIT_DEFAULT_TIMEOUT_NS
        const val EPOCH_INTERVAL = KChunksDefaults.CONCURRENCY_LIMIT_EPOCH_INTERVAL_NS
    }

    private val epochMetrics = DefaultEpochMetrics()
    private val mutex = Mutex()
    private val semaphore = AdjustableSemaphore()

    fun schedule(scope: CoroutineScope, initialWorkers: Int = INITIAL_LIMIT): Job = scope.launch {
        require(initialWorkers <= MAX_LIMIT) { "Initial concurrent limit $initialWorkers exceeded maximum concurrent limit $MAX_LIMIT" }
        require(initialWorkers >= MIN_LIMIT) { "Initial concurrent limit $initialWorkers is less than minimum concurrent limit $MIN_LIMIT" }

        var currentWorkersLimit = initialWorkers
        while (chunks.run { anyChunkInStartedState || anyChunkInDownloadingState || anyChunkInRetryState || anyChunkInUnknownState }) {
            semaphore.setPermits(currentWorkersLimit)
            launchWorkers(chunkQueue.size)
            delay(EPOCH_INTERVAL.nanoseconds)
            val inflightWorkers = calculateInflightWorkers()
            val metrics = epochMetrics.snapshotAndReset()
            val newWorkersLimit =
                getAIMDConcurrencyLimit(currentWorkersLimit, inflightWorkers, metrics.avgLatencyNs, metrics.didDrop)
            currentWorkersLimit = newWorkersLimit
            IOUtils.log("currentWorkersLimit: $currentWorkersLimit, inflight: $inflightWorkers, queueSize: ${chunkQueue.size}")
        }
    }

    private suspend fun CoroutineScope.launchWorkers(count: Int = 1) = repeat(count) {
        val chunk = mutex.withLock { chunkQueue.removeFirstOrNull() }
        chunk?.let { chunk ->
            val customHeaders = Headers.build {
                val range = "bytes=${chunk.currentOffset}-${chunk.endByte}"

                append("If-Match", chunk.etag)
                append("Range", range)
            }

            val job = launch(CoroutineName("#chunk-${chunk.id}-coroutine")) {
                try {
                    with(HttpService) {
                        semaphore.withPermit {
                            streamingDownload(
                                url = url,
                                chunkId = chunk.id,
                                chunkFilePath = chunk.filePath,
                                epochMetrics = epochMetrics,
                                rangeRequest = true,
                                customHeaders = customHeaders,
                                updateChunkStateFlow = updateChunkStateFlow
                            )
                        }
                    }

                    updateChunkStateFlow(chunk.id) {
                        copy(state = DownloadState.Done)
                    }
                } catch (e: Exception) {
                    when (e) {
                        is CancellationException -> {
                            updateChunkStateFlow(chunk.id) {
                                copy(state = DownloadState.Cancelled)
                            }

                            throw e
                        }

                        is RetryableNetworkException -> {
                            // add chunk again to the queue on failure
                            mutex.withLock { chunkQueue.addLast(chunk) }
                            epochMetrics.record(0.seconds, false)
                            updateChunkStateFlow(chunk.id) {
                                copy(state = DownloadState.Retry)
                            }
                            IOUtils.log(
                                coroutineContext,
                                "Streaming download failed with ${e.message}, added to queue again for retry after: ${e.retryAfter}"
                            )

                            return@launch
                        }

                        else -> {
                            updateChunkStateFlow(chunk.id) {
                                copy(state = DownloadState.Failed)
                            }
                            IOUtils.log(
                                coroutineContext,
                                "Streaming download failed with ${e.message}"
                            )

                            if (e !is NonRetryableNetworkException)
                                throw e
                        }
                    }
                }
            }

            updateChunkStateFlow(chunk.id) {
                copy(job = job, state = DownloadState.Started)
            }
        }
    }

    private fun calculateInflightWorkers() = chunks.value
        .values
        .filter { it.state is DownloadState.Downloading }
        .size

    private fun getAIMDConcurrencyLimit(currentLimit: Int, inflight: Int, latency: Long, didDrop: Boolean): Int {
        var newLimit = currentLimit
        if (didDrop || latency > TIMEOUT) {
            newLimit = (currentLimit * BACKOFF_RATIO).toInt()
        } else if (inflight * 2 >= currentLimit) {
            newLimit = currentLimit + 1;
        }

        return min(
            MAX_LIMIT,
            max(MIN_LIMIT, newLimit)
        )
    }
}