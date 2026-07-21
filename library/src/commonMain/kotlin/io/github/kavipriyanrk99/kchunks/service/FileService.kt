package io.github.kavipriyanrk99.kchunks.service

import okio.FileSystem
import okio.Path
import okio.Path.Companion.toPath
import okio.SYSTEM

object FileService {
    private const val CURRENT_DIRECTORY = "./"

    fun validateDirPath(dirPath: Path): Boolean = FileSystem.SYSTEM.metadata(dirPath).isDirectory

    fun computeFileSize(filePath: Path): Long {
        val fs = FileSystem.SYSTEM
        if (!fs.exists(filePath))
            return 0L
        return fs.metadata(filePath).size ?: 0L
    }

    fun combineFiles(targetFileName: String, fileParts: List<Path>) {
        require(targetFileName.isNotBlank()) { "Target filename is empty" }
        require(fileParts.isNotEmpty()) { "Cannot combine an empty list of file parts." }

        val fs = FileSystem.SYSTEM
        val parentDir = fileParts.first().normalized().parent ?: CURRENT_DIRECTORY.toPath()
        val targetFilePath = parentDir.resolve(targetFileName)

        fileParts.forEach { part ->
            if (!fs.metadata(part).isRegularFile)
                error("Combining files failed: '$part' is not a regular file")

            if (part.normalized() == targetFilePath)
                error("Target filename '$targetFileName' must not match any file part name")
        }

        val tempTargetFilePath = parentDir.resolve("$targetFileName.tmp")
        fs.write(tempTargetFilePath) {
            fileParts.forEach { part ->
                fs.read(part) {
                    readAll(this@write)
                }
            }
        }

        fs.atomicMove(tempTargetFilePath, targetFilePath)
        fileParts.forEach(fs::delete)
    }

    fun cleanFiles(fileParts: List<Path>) {
        require(fileParts.isNotEmpty()) { "Cannot clean an empty list of file parts." }
        val fs = FileSystem.SYSTEM
        fileParts.forEach(fs::delete)
    }
}