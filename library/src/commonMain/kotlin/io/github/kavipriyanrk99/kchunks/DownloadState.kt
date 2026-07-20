package io.github.kavipriyanrk99.kchunks

sealed interface DownloadState {
    object Started: DownloadState
    object Downloading: DownloadState
    object Paused: DownloadState
    object Done: DownloadState
    object Cancelled: DownloadState
    object Failed: DownloadState
    object Retry: DownloadState
    object Unknown: DownloadState
}