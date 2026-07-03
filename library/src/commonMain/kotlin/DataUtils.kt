import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.DurationUnit

object DataUtils {

    fun calculateDownloadSpeed(downloadSamples: ArrayDeque<DownloadSample>): Double {
        val totalBytes = downloadSamples.sumOf { it.bytes.bytes }
        val totalDurationInMillis = downloadSamples.fold(Duration.ZERO) { currentTotal, sample ->
            currentTotal + sample.timeElapsed
        }.toDouble(DurationUnit.MILLISECONDS)
        val oneSecInMillis = 1000

        if(totalDurationInMillis <= 0.0) return 0.0
        return (totalBytes * oneSecInMillis / totalDurationInMillis)
    }

    fun calculateDownloadETA(totalSize: DataSize, downloadedDataSize: DataSize, speed: Double): Double {
        val remDownloadSize = totalSize.bytes - downloadedDataSize.bytes
        return remDownloadSize / speed
    }

    fun calculateDownloadPercentage(totalSize: DataSize, downloadedDataSize: DataSize): Double {
        return downloadedDataSize.bytes.toDouble() * 100 / totalSize.bytes
    }
}