import io.ktor.http.*
import io.ktor.utils.io.charsets.*

object HttpUtils {
    private const val CONTENT_DISPOSITION_DELIMITER = ';'
    private const val EQUAL_DELIMITER = '='
    private const val DOUBLE_QUOTES_DELIMITER = '"'
    private const val SINGLE_QUOTES_DELIMITER = '\''
    private const val CONTENT_DISPOSITION_ATTACHMENT = "attachment"
    private const val CONTENT_DISPOSITION_FILENAME_PARAM1 = "filename*="
    private const val CONTENT_DISPOSITION_FILENAME_PARAM2 = "filename="

    // From ktor library: response.headers.getSplitValues()
    fun splitHeaderValue(value: String, separator: Char, splitInsideQuotes: Boolean): List<String> {
        if (splitInsideQuotes) {
            return value.split(separator).map { it.trim() }.filter { it.isNotEmpty() }
        }

        val result = mutableListOf<String>()
        var start = 0
        var i = 0
        var inQuotes = false
        var escape = false

        fun emit(start: Int, end: Int) {
            val token = value.substring(start, end).trim()
            if (token.isNotEmpty()) result.add(token)
        }

        while (i < value.length) {
            val ch = value[i]
            if (inQuotes) {
                when {
                    escape -> escape = false
                    ch == '\\' -> escape = true
                    ch == '"' -> inQuotes = false
                }
                i++
                continue
            }

            when (ch) {
                '"' -> {
                    inQuotes = true
                    i++
                }

                separator -> {
                    emit(start, i)
                    i++
                    start = i
                }

                else -> i++
            }
        }

        emit(start, value.length)
        return result
    }

    fun prepareFileNameFromContentDisposition(contentDisposition: String): String {
        val parts = splitHeaderValue(contentDisposition, CONTENT_DISPOSITION_DELIMITER, false)
        val newParts = mutableListOf<String>()
        for (part in parts) {
            if (part.startsWith(CONTENT_DISPOSITION_FILENAME_PARAM1, true).not()
                && part.startsWith(CONTENT_DISPOSITION_FILENAME_PARAM2, true).not()
                && part.startsWith(CONTENT_DISPOSITION_ATTACHMENT, true).not()
            ) {
                var merged = part
                if (newParts.isNotEmpty()
                    && (newParts.last().startsWith(CONTENT_DISPOSITION_FILENAME_PARAM1, true)
                            || newParts.last().startsWith(CONTENT_DISPOSITION_FILENAME_PARAM2, true))
                ) {
                    merged = newParts.removeLast() + CONTENT_DISPOSITION_DELIMITER + merged
                }

                newParts.add(merged)
            } else {
                newParts.add(part)
            }
        }

        // handling filename* param
        val firstPartWithParam1 = newParts.firstOrNull { it.startsWith(CONTENT_DISPOSITION_FILENAME_PARAM1, true) }
        if (firstPartWithParam1 != null) {
            val encoded = firstPartWithParam1.substringAfter(CONTENT_DISPOSITION_FILENAME_PARAM1)
                .trim()
                .removeSurrounding(DOUBLE_QUOTES_DELIMITER.toString())
            val firstQuoteIndex = encoded.indexOf(SINGLE_QUOTES_DELIMITER)
            val secondQuoteIndex = encoded.indexOf(SINGLE_QUOTES_DELIMITER, firstQuoteIndex + 1)

            if (firstQuoteIndex > 0 && secondQuoteIndex > firstQuoteIndex) {
                val charset = encoded.substring(0, firstQuoteIndex)
                val language = encoded.substring(firstQuoteIndex + 1, secondQuoteIndex)
                val filename = encoded.substring(secondQuoteIndex + 1)

                val charsetInstance = if(charset == Charsets.ISO_8859_1.name || charset == Charsets.ISO_8859_1.name.lowercase()) {
                    Charsets.ISO_8859_1
                } else {
                    Charsets.UTF_8
                }

                if(filename.isNotBlank() && charset.isNotBlank() && Charsets.isSupported(charsetInstance.name)) {
                    return filename.decodeURLPart(charset = charsetInstance)
                }
            }
        }

        // handling filename param
        val firstPartWithParam2 = newParts.firstOrNull { it.startsWith(CONTENT_DISPOSITION_FILENAME_PARAM2, true) }
        if (firstPartWithParam2 != null) {
            val fileName = firstPartWithParam2.substringAfter(EQUAL_DELIMITER)
                .trim()
                .removeSurrounding(DOUBLE_QUOTES_DELIMITER.toString())

            if (fileName.isNotBlank())
                return fileName
        }

        return ""
    }

    fun prepareFileNameFromURL(url: String): String {

        return ""
    }
}