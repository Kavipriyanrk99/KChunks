package io.github.kavipriyanrk99.kchunks.utils

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class AdjustableSemaphore(initialPermits: Int) {
    init {
        require(initialPermits >= 0) { "Permits can't be negative" }
    }

    private val mutex = Mutex()
    private val queue = ArrayDeque<CompletableDeferred<Unit>>()
    private var permits = initialPermits
    private var acquired = 0

    suspend fun <T> withPermit(action: suspend () -> T): T {
        acquire()
        return try {
            action()
        } finally {
            release()
        }
    }

    /**
     * Adjusts the maximum number of concurrently held permits.
     *
     * If the new limit is lower than the number of permits currently held,
     * existing holders are unaffected. New acquisitions will block until
     * enough permits have been released for the number of held permits to
     * fall below the new limit.
     */
    suspend fun setPermits(permits: Int) = mutex.withLock {
        require(permits >= 0) { "Permits can't be negative" }
        this.permits = permits
        drainQueue()
    }

    private suspend fun acquire() {
        val waiter: CompletableDeferred<Unit>
        mutex.withLock {
            if (acquired < permits) {
                acquired++
                return
            }

            waiter = CompletableDeferred()
            queue.add(waiter)
        }

        try {
            waiter.await()
        } catch (ce: CancellationException) {
            mutex.withLock {
                if (queue.remove(waiter)) {
                    // Still queued, never got a permit.
                } else {
                    // Already been dequeued, so a permit had been
                    // accounted for in acquired.
                    acquired--
                    drainQueue()
                }
            }

            throw ce
        }
    }

    private suspend fun release() = mutex.withLock {
        acquired--
        drainQueue()
    }

    /**
     * Caller must hold mutex.
     */
    private fun drainQueue() {
        while (queue.isNotEmpty() && acquired < permits) {
            if (queue.removeFirst().complete(Unit))
                acquired++
        }
    }
}