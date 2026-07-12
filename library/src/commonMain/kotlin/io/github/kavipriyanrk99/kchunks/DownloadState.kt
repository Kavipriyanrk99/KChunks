package io.github.kavipriyanrk99.kchunks

sealed interface DownloadState {
    object Started: DownloadState
    object Downloading: DownloadState
    object Paused: DownloadState
    object Done: DownloadState
    object Failed: DownloadState
    object Unknown: DownloadState
}