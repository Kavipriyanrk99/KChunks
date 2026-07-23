package io.github.kavipriyanrk99.kchunks

import io.github.kavipriyanrk99.kchunks.service.FileService
import okio.Path

class KChunks(val url: String, val dirPath: Path) {
    init {
        require(url.isNotBlank()) { "Download URL is blank" }
        require(FileService.validateDirPath(dirPath)) { "Directory path is not a directory" }
    }

    private val downloader = Downloader(url, dirPath)

    suspend fun download() {
        downloader.multipartDownload()
    }

    suspend fun pause(): Boolean {
        val stat = downloader.pause()
        downloader.chunks.value.values.forEach { println("${it.id}:${it.state}") }
        return stat
    }

    suspend fun resume() {
        downloader.resume()
        downloader.chunks.value.values.forEach { println("${it.id}:${it.state}") }
    }

    suspend fun cancel(): Boolean {
        val stat = downloader.cancel()
        downloader.chunks.value.values.forEach { println("${it.id}:${it.state}") }
        return stat
    }
}