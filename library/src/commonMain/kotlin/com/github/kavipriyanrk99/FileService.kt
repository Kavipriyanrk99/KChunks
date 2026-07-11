package com.github.kavipriyanrk99

import okio.FileSystem
import okio.Path
import okio.SYSTEM

object FileService {
    fun validateDirPath(dirPath: Path): Boolean = FileSystem.SYSTEM.metadata(dirPath).isDirectory
}