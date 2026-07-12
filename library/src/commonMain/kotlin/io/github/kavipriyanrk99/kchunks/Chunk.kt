package io.github.kavipriyanrk99.kchunks

import kotlinx.coroutines.Job
import okio.Path

data class Chunk(
    val name: String,
    val startByte: Long,
    val endByte: Long,
    val currentOffset: Long,
    val etag: String,
    val job: Job? = null,
    val state: DownloadState = DownloadState.Unknown,
    val speed: Double? = null,
    val eta: Double? = null,
    val percentage: Double = 0.0,
    val filePath: Path
)
