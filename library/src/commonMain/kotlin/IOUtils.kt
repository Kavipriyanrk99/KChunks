import kotlinx.coroutines.CoroutineName
import okio.FileSystem
import okio.Path
import okio.SYSTEM
import kotlin.coroutines.CoroutineContext
import kotlin.time.Clock

object IOUtils {
    fun sanitizeFileName(fileName: String): String {
        return fileName
    }

    fun prepareDefaultFileName(): String {
        return "kchunks-" + Clock.System.now()
    }

    fun findFileSizeInBytes(filePath: Path) = FileSystem.SYSTEM.metadata(filePath).size

    fun <T> log(coroutineContext: CoroutineContext, msg: T) {
        val coroutineName = coroutineContext[CoroutineName]?.name
        println("[$coroutineName] ${msg.toString()}")
    }

    fun <T> log(msg: T) = println(msg.toString())
}