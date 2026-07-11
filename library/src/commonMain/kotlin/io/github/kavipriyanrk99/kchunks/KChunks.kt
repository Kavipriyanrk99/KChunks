package io.github.kavipriyanrk99.kchunks

import okio.Path

class KChunks(val url: String, val dirPath: Path) {
    init {
        require(url.isNotBlank()) { "Download URL is blank" }
        require(FileService.validateDirPath(dirPath)) { "Directory path is not a directory" }
    }

    suspend fun download() {
        TODO()
    }

    suspend fun pause() {
        TODO()
    }

    suspend fun resume() {
        TODO()
    }

    suspend fun cancel() {
        TODO()
    }
}