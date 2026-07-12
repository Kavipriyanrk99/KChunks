package io.github.kavipriyanrk99.kchunks.service

import okio.FileSystem
import okio.Path
import okio.SYSTEM

object FileService {
    fun validateDirPath(dirPath: Path): Boolean = FileSystem.SYSTEM.metadata(dirPath).isDirectory
}