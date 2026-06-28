class DataSize(val bytes: Long) {
    companion object {
        const val ZERO_BYTES = 0L
        const val BASE_BYTES = 1000L
        inline val Long.bytes: DataSize get() = DataSize(this)
        inline val Long.kilobytes: DataSize get() = DataSize(this * BASE_BYTES)
        inline val Long.megabytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES)
        inline val Long.gigabytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES * BASE_BYTES)
        inline val Long.terabytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES * BASE_BYTES * BASE_BYTES)
        inline val Long.petabytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES * BASE_BYTES * BASE_BYTES * BASE_BYTES)
    }

    fun toLong() = bytes
    fun equalTo(other: DataSize) = this.bytes == other.bytes
    fun lessThanOrEqualTo(other: DataSize) = this.bytes <= other.bytes
    fun lessThan(other: DataSize) = this.bytes < other.bytes
    fun greaterThan(other: DataSize) = this.bytes > other.bytes
    fun greaterThanOrEqualTo(other: DataSize) = this.bytes >= other.bytes
}