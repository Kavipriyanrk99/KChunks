import kotlin.math.roundToLong

/**
 * Represents a non-negative quantity of data.
 *
 * Internally, all values are stored as a [Long] number of bytes. Since bytes are
 * the smallest supported unit, the largest representable value is [Long.MAX_VALUE]
 * bytes.
 *
 * Factory extensions such as `Long.kibibytes` and `Double.gibibytes` perform
 * overflow checks before creating a [DataSize]. Any value that would exceed
 * [Long.MAX_VALUE] bytes results in an [IllegalArgumentException].
 */
class DataSize(val bytes: Long): Comparable<DataSize> {
    companion object {
        internal const val ZERO_BYTES = 0L
        internal const val KIBIBYTES = 1024L
        internal const val MEBIBYTES = KIBIBYTES * KIBIBYTES
        internal const val GIBIBYTES = MEBIBYTES * KIBIBYTES
        internal const val TEBIBYTES = GIBIBYTES * KIBIBYTES
        internal const val PEBIBYTES = TEBIBYTES * KIBIBYTES
        internal const val MAX_BYTES = Long.MAX_VALUE
        internal const val MAX_KIBIBYTES = MAX_BYTES / KIBIBYTES
        internal const val MAX_MEBIBYTES = MAX_BYTES / MEBIBYTES
        internal const val MAX_GIBIBYTES = MAX_BYTES / GIBIBYTES
        internal const val MAX_TEBIBYTES = MAX_BYTES / TEBIBYTES
        internal const val MAX_PEBIBYTES = MAX_BYTES / PEBIBYTES

        private fun Long.toDataSize(unit: Long, max: Long): DataSize {
            require(this >= 0) { "DataSize can't be negative" }
            require(this <= max) { "DataSize overflow. DataSize handles only a maximum size of $MAX_BYTES bytes" }
            return DataSize(this * unit)
        }

        private fun Double.toDataSize(unit: Long): DataSize {
            val bytes = this * unit
            require(bytes.isFinite()) { "DataSize should be finite" }
            require(bytes >= 0) { "DataSize can't be negative" }
            require(bytes <= MAX_BYTES.toDouble()) { "DataSize overflow. DataSize handles only a maximum size of $MAX_BYTES bytes" }
            return DataSize(bytes.roundToLong())
        }

        val Long.bytes: DataSize get() = toDataSize(1, MAX_BYTES)

        val Long.kibibytes: DataSize get() = toDataSize(KIBIBYTES, MAX_KIBIBYTES)

        val Double.kibibytes: DataSize get() = toDataSize(KIBIBYTES)

        val Long.mebibytes: DataSize get() = toDataSize(MEBIBYTES, MAX_MEBIBYTES)

        val Double.mebibytes: DataSize get() = toDataSize(MEBIBYTES)

        val Long.gibibytes: DataSize get() = toDataSize(GIBIBYTES, MAX_GIBIBYTES)

        val Double.gibibytes: DataSize get() = toDataSize(GIBIBYTES)

        val Long.tebibytes: DataSize get() = toDataSize(TEBIBYTES, MAX_TEBIBYTES)

        val Double.tebibytes: DataSize get() = toDataSize(TEBIBYTES)

        val Long.pebibytes: DataSize get() = toDataSize(PEBIBYTES, MAX_PEBIBYTES)

        val Double.pebibytes: DataSize get() = toDataSize(PEBIBYTES)
    }

    override fun toString(): String = bytes.toString()

    override fun compareTo(other: DataSize): Int = when {
        this.bytes == other.bytes -> 0
        this.bytes < other.bytes -> -1
        else -> 1
    }
}