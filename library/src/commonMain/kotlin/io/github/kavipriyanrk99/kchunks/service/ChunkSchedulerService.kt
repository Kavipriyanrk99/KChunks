package io.github.kavipriyanrk99.kchunks.service

import io.github.kavipriyanrk99.kchunks.Chunk
import io.github.kavipriyanrk99.kchunks.DownloadState
import io.github.kavipriyanrk99.kchunks.KChunksDefaults
import io.github.kavipriyanrk99.kchunks.utils.HttpUtils
import io.ktor.http.Headers
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.seconds

class ChunkSchedulerService(
    private val url: String,
    private val chunkQueue: ArrayDeque<Chunk>,
    private val chunks: StateFlow<Map<String, Chunk>>,
    val updateChunkStateFlow: (chunkName: String, transform: Chunk.() -> Chunk) -> Unit
) {

    suspend fun schedule(initialWorkers: Int = KChunksDefaults.DEFAULT_WORKERS) = coroutineScope {
        var currentWorkers = initialWorkers
        var optimalWorkers = initialWorkers
        launchWorkers(currentWorkers)

        launch {
            // waits till chunks in downloadState goes to DownloadState.Download
            awaitChunksInDownloading(currentWorkers)
            var prevThroughput = HttpUtils.calculateCurrentThroughput(chunks.value.values.toList())
            var currentThroughput: Double
            while (chunkQueue.isNotEmpty()) {
                delay(10.seconds)

                currentWorkers = calculateCurrentWorkers()
                if (currentWorkers != 0)
                    optimalWorkers = currentWorkers

                currentThroughput = HttpUtils.calculateCurrentThroughput(chunks.value.values.toList())
                if (currentWorkers == 0)
                    launchWorkers(optimalWorkers)
                else {
                    if ((currentThroughput - prevThroughput) / prevThroughput >= 0.05) {
                        launchWorkers()
                        currentWorkers += 1
                    } else {
                        currentWorkers /= 2
                        killSlowWorkers(currentWorkers)
                    }
                }

                prevThroughput = currentThroughput
                IOUtils.log("Optimal workers: $optimalWorkers, current workers: $currentWorkers")
            }
        }
    }

    private fun CoroutineScope.launchWorkers(count: Int = 1) = repeat(count) {
        val chunk = chunkQueue.removeFirstOrNull()
        chunk?.let { chunk ->
            val customHeaders = Headers.build {
                val range = "bytes=${chunk.currentOffset}-${chunk.endByte}"

                append("If-Match", chunk.etag)
                append("Range", range)
            }

            val job = launch {
                with(HttpService) {
                    try {
                        streamingDownload(
                            url = url,
                            chunkName = chunk.name,
                            chunkFilePath = chunk.filePath,
                            customHeaders = customHeaders,
                            updateChunkStateFlow = updateChunkStateFlow
                        )
                    } catch (ce: CancellationException) {
                        throw CancellationException(ce)
                    } catch (e: Exception) {
                        // add chunk again to the queue on failure
                        chunkQueue.addLast(chunk)
                        throw Exception(e)
                    }

                }
            }

//            chunks.update { oldChunks ->
//                oldChunks + (chunk.name to oldChunks[chunk.name]!!.copy(
//                    job = job,
//                    state = DownloadState.Started
//                ))
//            }
            updateChunkStateFlow(chunk.name) {
                copy(job = job, state = DownloadState.Started)
            }
        }
    }

    private fun killSlowWorkers(count: Int) = repeat(count) {
        val slowState = chunks.value
            .values
            .filter { it.state is DownloadState.Downloading }
            .minByOrNull {
                it.speed ?: 0.0
            }

        slowState?.let { slowState ->
            slowState.job?.cancel()
        }
//        val slowState = _downloadStateMultipart.value.minByOrNull {
//            (it.value as? DownloadState.Downloading)?.percentage ?: 0.0
//        }
//
//        if (slowState != null) {
//            val coroutineName = jobCoroutineNameMap[slowState.key]
//            slowState.key.cancel()
//            _downloadStateMultipart.update { old ->
//                old.toMutableMap().apply {
//                    remove(slowState.key)
//                }
//            }
//            tempChunks.remove(coroutineName)?.let {
//                chunks[coroutineName!!] = it
//            }
//            IOUtils.log("Killing worker of chunk: $coroutineName")
//        }
    }

    private suspend fun awaitChunksInDownloading(currentWorkers: Int) = chunks
        .first { map ->
            map.isNotEmpty() && map.filter { it.value.state is DownloadState.Downloading }.size == currentWorkers
        }

    private fun calculateCurrentWorkers() = chunks.value
        .values
        .filter { it.state is DownloadState.Downloading }
        .size
}