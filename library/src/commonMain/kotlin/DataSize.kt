class DataSize(val bytes: Long) {
    companion object {
        const val ZERO_BYTES = 0L
        const val BASE_BYTES = 1024L
        inline val Long.bytes: DataSize get() = DataSize(this)
        inline val Long.kibibytes: DataSize get() = DataSize(this * BASE_BYTES)
        inline val Long.mebibytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES)
        inline val Long.gibibytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES * BASE_BYTES)
        inline val Long.tebibytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES * BASE_BYTES * BASE_BYTES)
        inline val Long.pebibytes: DataSize get() = DataSize(this * BASE_BYTES * BASE_BYTES * BASE_BYTES * BASE_BYTES * BASE_BYTES)
    }

    fun toLong() = bytes
    fun equalTo(other: DataSize) = this.bytes == other.bytes
    fun lessThanOrEqualTo(other: DataSize) = this.bytes <= other.bytes
    fun lessThan(other: DataSize) = this.bytes < other.bytes
    fun greaterThan(other: DataSize) = this.bytes > other.bytes
    fun greaterThanOrEqualTo(other: DataSize) = this.bytes >= other.bytes
}