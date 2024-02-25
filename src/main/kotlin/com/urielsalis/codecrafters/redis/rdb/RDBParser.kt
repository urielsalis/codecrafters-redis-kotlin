package com.urielsalis.codecrafters.redis.rdb

import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.io.InputStream
import java.nio.ByteBuffer
import java.time.Instant

fun parseRDB(input: ByteArray): Map<String, Pair<Instant, RespMessage>> {
    val inputStream = input.inputStream()
    val magic = inputStream.readNBytes(5)
    if (!magic.contentEquals("REDIS".toByteArray())) {
        throw Exception("Invalid RDB file")
    }
    val version = inputStream.readNBytes(4)
    println("Version: ${version.decodeToString()}")
    var type = inputStream.read()
    while (true) {
        when (type) {
            0xFA -> {
                val key = readStringEncoded(inputStream)
                val value = readStringEncoded(inputStream)
                println("Auxiliary Key: $key, Value: $value")
            }

            0xFE -> {
                println("Database selector: ${inputStream.read()}")
            }

            0xFB -> {
                val hashTableSize = readLengthEncoded(inputStream)
                val expireHashTableSize = readLengthEncoded(inputStream)
                println("Resize DB to $hashTableSize, $expireHashTableSize")
            }

            0xFC -> {
                TODO("Expiry")
            }

            0xFD -> {
                TODO("Expiry")
            }

            else -> break
        }
        type = inputStream.read()
    }
    // Got to keys, type contains the key type
    val keys = mutableMapOf<String, Pair<Instant, RespMessage>>()
    while (type != 0xFF) {
        val key = readStringEncoded(inputStream)
        val value = when (type) {
            0 -> readStringEncoded(inputStream)
            else -> throw Exception("Invalid encoding type: $type")
        }
        keys[key] = Instant.MAX to BulkStringRespMessage(value)
        type = inputStream.read()
    }
    return keys
}

private fun readStringEncoded(inputStream: InputStream): String {
    val length = readLengthEncoded(inputStream)
    if (length == 0) {
        return ""
    }
    val chars = inputStream.readNBytes(length)
    return chars.decodeToString()
}

private fun readLengthEncoded(inputStream: InputStream): Int {
    val value = inputStream.readNBytes(1)[0]
    val masked = (value.toUInt() and 0xc0u) shr 6
    if (masked == 0u) {
        return value.toInt()
    }
    if (masked == 1u) {
        val firstValue = (value.toInt() and 63.toByte().toInt()).toByte()
        val secondValue = inputStream.readNBytes(1)[0]
        return ((firstValue.toInt() shl 8) or (secondValue.toInt() and 0xFF)).toShort().toInt()
    }
    if (masked == 2u) {
        val bytes = inputStream.readNBytes(4)
        return ByteBuffer.wrap(bytes).getInt()
    }
    if (masked == 3u) {
        // Special cases are unsupported, we read the data and return 0
        val type = value.toUInt() and 0x3fu
        when (type) {
            0u -> { // Signed integer, 8 bits
                inputStream.readNBytes(1)
                return 0
            }

            1u -> { // Signed integer, 16 bits
                inputStream.readNBytes(2)
                return 0
            }

            2u -> { // Signed integer, 32 bits
                inputStream.readNBytes(4)
                return 0
            }

            3u -> throw UnsupportedOperationException("Cant read special LZF string$type")
            else -> throw UnsupportedOperationException("Unknown special encoding$type")
        }
    }
    throw UnsupportedOperationException("Dont know how to read special legnth encodings")
}

