package io.github.kavipriyanrk99.kchunks

import io.github.kavipriyanrk99.kchunks.service.FileService
import kotlinx.coroutines.Job
import okio.Path

data class Chunk(
    val id: Int,
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
) {
    fun computeUpdatedCurrentOffset(): Long {
        val fileSize = FileService.computeFileSize(filePath)
        val chunkSize = endByte - startByte + 1
        check(chunkSize >= fileSize) {
            "Chunk size: $chunkSize should " +
                    "be greater than or equal to chunk filesize: $fileSize"
        }

        return if (chunkSize == fileSize) {
            endByte
        } else startByte + fileSize
    }
}
