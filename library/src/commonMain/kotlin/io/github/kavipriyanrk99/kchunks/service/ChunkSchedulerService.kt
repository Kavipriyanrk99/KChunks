package io.github.kavipriyanrk99.kchunks.service

import io.github.kavipriyanrk99.kchunks.Chunk
import io.github.kavipriyanrk99.kchunks.DownloadState
import io.github.kavipriyanrk99.kchunks.EpochMetrics
import io.github.kavipriyanrk99.kchunks.KChunksDefaults
import io.github.kavipriyanrk99.kchunks.utils.AdjustableSemaphore
import io.ktor.http.*
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
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
    private val chunks: StateFlow<Map<String, Chunk>>,
    val updateChunkStateFlow: (chunkName: String, transform: Chunk.() -> Chunk) -> Unit
) {
    private companion object {
        const val MIN_LIMIT = KChunksDefaults.CONCURRENCY_LIMIT_MIN_LIMIT
        const val INITIAL_LIMIT = KChunksDefaults.CONCURRENCY_LIMIT_INITIAL_LIMIT
        const val MAX_LIMIT = KChunksDefaults.CONCURRENCY_LIMIT_MAX_LIMIT
        const val BACKOFF_RATIO = KChunksDefaults.CONCURRENCY_LIMIT_BACKOFF_RATIO
        const val TIMEOUT = KChunksDefaults.CONCURRENCY_LIMIT_DEFAULT_TIMEOUT_NS
        const val EPOCH_INTERVAL = KChunksDefaults.CONCURRENCY_LIMIT_EPOCH_INTERVAL_NS
    }

    private val epochMetrics = EpochMetrics()
    private val mutex = Mutex()
    private val semaphore = AdjustableSemaphore()

    suspend fun schedule(initialWorkers: Int = INITIAL_LIMIT) = coroutineScope {
        require(initialWorkers <= MAX_LIMIT) { "Initial concurrent limit $initialWorkers exceeded maximum concurrent limit $MAX_LIMIT" }
        require(initialWorkers >= MIN_LIMIT) { "Initial concurrent limit $initialWorkers is less than minimum concurrent limit $MIN_LIMIT" }

        launch {
            var currentWorkersLimit = initialWorkers
            while (areAllChunksDownloaded.not()) {
                semaphore.setPermits(currentWorkersLimit)
                launchWorkers(chunkQueue.size)
                delay(EPOCH_INTERVAL.nanoseconds)
                val inflightWorkers = calculateInflightWorkers()
                val metrics = epochMetrics.snapshotAndReset()
                val newWorkersLimit =
                    getAIMDConcurrencyLimit(currentWorkersLimit, inflightWorkers, metrics.avgLatencyNs, metrics.didDrop)
                currentWorkersLimit = newWorkersLimit
            }
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

            val job = launch(CoroutineName("#${chunk.name}-coroutine")) {
                with(HttpService) {
                    semaphore.withPermit {
                        try {
                            streamingDownload(
                                url = url,
                                chunkName = chunk.name,
                                chunkFilePath = chunk.filePath,
                                epochMetrics = epochMetrics,
                                customHeaders = customHeaders,
                                updateChunkStateFlow = updateChunkStateFlow
                            )
                        } catch (ce: CancellationException) {
                            throw ce
                        } catch (e: Exception) {
                            // add chunk again to the queue on failure
                            mutex.withLock { chunkQueue.addLast(chunk) }
                            updateChunkStateFlow(chunk.name) {
                                copy(state = DownloadState.Failed)
                            }
                            epochMetrics.record(0.seconds, false)
                            throw e
                        }
                    }
                }

                updateChunkStateFlow(chunk.name) {
                    copy(state = DownloadState.Done)
                }
            }

            updateChunkStateFlow(chunk.name) {
                copy(job = job, state = DownloadState.Started)
            }
        }
    }

    private val areAllChunksDownloaded
        get() = chunks.value
            .values
            .all { it.state is DownloadState.Done }

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