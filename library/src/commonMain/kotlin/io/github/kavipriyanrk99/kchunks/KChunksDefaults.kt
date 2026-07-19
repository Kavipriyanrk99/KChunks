package io.github.kavipriyanrk99.kchunks

import io.ktor.client.HttpClient
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.HttpTimeoutConfig

internal object KChunksDefaults {
    val defaultHttpClient by lazy {
        HttpClient {
            install(HttpTimeout) {
                requestTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
                connectTimeoutMillis = 10_000
                socketTimeoutMillis = 30_000
            }
        }
    }

    const val DEFAULT_WORKERS = 4
    const val DEFAULT_NO_OF_CHUNKS = 32
    const val DEFAULT_CHUNK_SIZE = 1024 * 1024 * 2L // 2 MebiBytes
    const val DEFAULT_BUFFER_SIZE = 1024 * 64L // 64 KibiBytes
    const val DEFAULT_SAMPLE_SIZE = 10
    const val CONCURRENCY_LIMIT_DEFAULT_TIMEOUT_NS = 10_00_00_000 // 100 milliseconds
    const val CONCURRENCY_LIMIT_MIN_LIMIT = 2
    const val CONCURRENCY_LIMIT_INITIAL_LIMIT = 2
    const val CONCURRENCY_LIMIT_MAX_LIMIT = 32
    const val CONCURRENCY_LIMIT_BACKOFF_RATIO = 0.9
    const val CONCURRENCY_LIMIT_EPOCH_INTERVAL_NS = 3_00_00_00_000 // 3 seconds
}