package io.github.kavipriyanrk99.kchunks

sealed class KChunksException(message: String, cause: Throwable? = null) : Exception(message, cause)

sealed class NetworkException(message: String, cause: Throwable? = null) : KChunksException(message, cause)

class RetryableNetworkException(val retryAfter: String? = null, message: String, cause: Throwable? = null) : NetworkException(message, cause)

class NonRetryableNetworkException(message: String, cause: Throwable? = null) : NetworkException(message, cause)