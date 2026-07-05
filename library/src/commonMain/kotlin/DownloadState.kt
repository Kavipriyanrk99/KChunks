import kotlin.time.Duration

sealed interface DownloadState {
    object Started: DownloadState
    data class Downloading(val speed: Double, val eta: Double, val percentage: Double): DownloadState
    object Paused: DownloadState
    object Done: DownloadState
    object Failed: DownloadState
    object Unknown: DownloadState
}