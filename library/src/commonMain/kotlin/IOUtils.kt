import okio.FileSystem
import okio.Path
import okio.SYSTEM
import kotlin.time.Clock

object IOUtils {
    fun sanitizeFileName(fileName: String): String {
        return fileName
    }

    fun prepareDefaultFileName(): String {
        return "kchunks-" + Clock.System.now()
    }

    fun findFileSizeInBytes(filePath: Path) = FileSystem.SYSTEM.metadata(filePath).size

}