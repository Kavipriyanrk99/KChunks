import kotlin.time.Clock

object IOUtils {
    fun sanitizeFileName(fileName: String): String {
        return fileName
    }

    fun prepareDefaultFileName(): String {
        return "kchunks-" + Clock.System.now()
    }
}