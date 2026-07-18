package io.github.kavipriyanrk99.kchunks

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.DurationUnit


class EpochMetrics {
    data class Metrics(val avgLatencyNs: Long, val didDrop: Boolean)

    private val mutex = Mutex()
    private var totalLatencyNs = 0L
    private var totalSample = 0
    private var totalFailures = 0

    suspend fun record(latency: Duration, success: Boolean = true) = mutex.withLock {
        totalSample += 1
        totalLatencyNs += latency.toLong(DurationUnit.NANOSECONDS)
        if (success.not())
            totalFailures += 1
    }

    suspend fun snapshotAndReset(): Metrics = mutex.withLock {
        IOUtils.log("totalSample: $totalSample, totalFailures: $totalFailures, totalLatencyNs: $totalLatencyNs")
        if(totalSample <= 0) {
            IOUtils.log("Zero samples. Returning empty metrics")
            return Metrics(0,false)
        }
        val avgLatencyNs = totalLatencyNs / (totalSample - totalFailures)
        val didDrop = totalFailures > 0

        totalLatencyNs = 0
        totalSample = 0
        totalFailures = 0
        return Metrics(avgLatencyNs, didDrop)
    }
}