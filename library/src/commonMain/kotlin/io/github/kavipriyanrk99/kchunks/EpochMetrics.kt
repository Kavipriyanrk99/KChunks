package io.github.kavipriyanrk99.kchunks

import IOUtils
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.DurationUnit

sealed interface EpochMetrics {
    data class Metrics(val avgLatencyNs: Long, val didDrop: Boolean)

    suspend fun record(latency: Duration, success: Boolean = true)
    suspend fun snapshotAndReset(): Metrics
}

object NoOpEpochMetrics: EpochMetrics {
    override suspend fun record(latency: Duration, success: Boolean) = Unit
    override suspend fun snapshotAndReset(): EpochMetrics.Metrics = EpochMetrics.Metrics(0, false)
}

class DefaultEpochMetrics: EpochMetrics {
    private val mutex = Mutex()
    private var totalLatencyNs = 0L
    private var totalSample = 0
    private var totalFailures = 0

    override suspend fun record(latency: Duration, success: Boolean ) = mutex.withLock {
        totalSample += 1
        totalLatencyNs += latency.toLong(DurationUnit.NANOSECONDS)
        if (success.not())
            totalFailures += 1
    }

    override suspend fun snapshotAndReset(): EpochMetrics.Metrics = mutex.withLock {
        IOUtils.log("totalSample: $totalSample, totalFailures: $totalFailures, totalLatencyNs: $totalLatencyNs")
        if(totalSample <= 0) {
            IOUtils.log("Zero samples. Returning empty metrics")
            return EpochMetrics.Metrics(0, false)
        }

        if(totalSample == totalFailures) {
            IOUtils.log("All samples failed. Returning drop metrics")
            return EpochMetrics.Metrics(0, true)
        }
        val avgLatencyNs = totalLatencyNs / (totalSample - totalFailures)
        val didDrop = totalFailures > 0

        totalLatencyNs = 0
        totalSample = 0
        totalFailures = 0
        return EpochMetrics.Metrics(avgLatencyNs, didDrop)
    }
}