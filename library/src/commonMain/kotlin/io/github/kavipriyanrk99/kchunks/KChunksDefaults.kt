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
    const val DEFAULT_NO_OF_CHUNKS = 16
    const val DEFAULT_CHUNK_SIZE = 1024 * 1024 * 2L // 2 MebiBytes
    const val DEFAULT_BUFFER_SIZE = 1024 * 16L // 16 KibiBytes
    const val DEFAULT_SAMPLE_SIZE = 10
}