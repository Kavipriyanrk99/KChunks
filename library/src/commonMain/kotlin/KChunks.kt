import DataSize.Companion.bytes
import DataSize.Companion.kibibytes
import DataSize.Companion.mebibytes
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.HttpTimeoutConfig
import io.ktor.client.request.get
import io.ktor.client.request.head
import io.ktor.client.request.prepareGet
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsBytes
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentLength
import io.ktor.http.isSuccess
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.core.remaining
import io.ktor.utils.io.exhausted
import io.ktor.utils.io.readRemaining
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.io.buffered
import kotlinx.io.readByteArray
import okio.FileSystem
import okio.Path
import okio.SYSTEM

class KChunks(private val url: String, private val path: Path) {
    private val httpClient by lazy {
        HttpClient {
            install(HttpTimeout) {
                requestTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
                connectTimeoutMillis = 10_000
                socketTimeoutMillis = 30_000
            }
        }
    }
    private val bufferSize = 8L.kibibytes
    private var job: Job? = null
    var contentLength: DataSize? = null
        private set

    private fun closeHttpClient() = httpClient.close()

    private suspend fun headRequest() {
        val res = httpClient.head(url)

        if (res.status == HttpStatusCode.OK)
            contentLength = res.contentLength()?.bytes
    }

    private fun HttpResponse.isSuccessful() = when {
        this.status.isSuccess() -> true
        else -> {
            println("Request failed with status code: ${this.status.value}, msg: ${this.status.description}")
            false
        }
    }

    private suspend fun streamingDownload() = httpClient.prepareGet(url).execute { response ->
        if (response.isSuccessful().not()) {
            return@execute
        }

        contentLength = response.contentLength()?.bytes

        val channel: ByteReadChannel = response.body()
        var readBytes = 0L

        FileSystem.SYSTEM.write(path) {
            while (!channel.exhausted()) {
                val chunk = channel.readRemaining(bufferSize.toLong())
                readBytes += chunk.remaining
                this.write(chunk.buffered().readByteArray())
                println("Received $readBytes bytes from ${contentLength?.toLong() ?: "UNKNOWN"}")
            }
        }
    }


    private suspend fun directDownload() {
        val res = httpClient.get(url)
        if (res.isSuccessful().not()) {
            return
        }

        FileSystem.SYSTEM.write(path) {
            val bodyAsBytes = res.bodyAsBytes()
            this.write(bodyAsBytes)
            println("Received ${bodyAsBytes.size} bytes from ${contentLength?.toLong() ?: "UNKNOWN"}")
        }
    }

    suspend fun download() = coroutineScope {
        job = launch {
            headRequest()

            when {
                contentLength == null
                        || contentLength!!.greaterThan(bufferSize) -> streamingDownload()

                else -> directDownload()
            }
            closeHttpClient()
        }
    }

    suspend fun cancel() {
        job?.cancelAndJoin()
        closeHttpClient()
    }
}