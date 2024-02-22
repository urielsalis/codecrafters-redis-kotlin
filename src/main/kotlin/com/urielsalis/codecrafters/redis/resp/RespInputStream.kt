package com.urielsalis.codecrafters.redis.resp

import java.io.Closeable
import java.io.InputStream

class RespInputStream(val stream: InputStream) : Closeable {
    fun read(): RespMessage? {
        return parse()
    }

    fun parse(): RespMessage? {
        val bytes = stream.readNBytes(1)
        if (bytes.isEmpty()) {
            return null
        }
        val header = bytes[0].toInt().toChar()
        return when (header) {
            '+' -> parseSimpleString()
            '-' -> parseSimpleError()
            ':' -> parseInteger()
            '$' -> parseBulkString()
            '*' -> parseArray()
            else -> throw Exception("Invalid RESP message")
        }
    }

    private fun readUntilNewLine(): String {
        val builder = StringBuilder()
        while (true) {
            val nextChar = stream.readNBytes(1)[0].toInt().toChar()
            if (nextChar == '\r') {
                stream.readNBytes(1)
                return builder.toString()
            }
            builder.append(nextChar)
        }
    }

    private fun parseSimpleString(): SimpleStringRespMessage {
        val parsed = readUntilNewLine()
        return SimpleStringRespMessage(parsed)
    }

    private fun parseSimpleError(): ErrorRespMessage {
        val parsed = readUntilNewLine()
        return ErrorRespMessage(parsed)
    }

    private fun parseInteger(): IntegerRespMessage {
        val parsed = readUntilNewLine()
        if (parsed.startsWith("-")) {
            return IntegerRespMessage(parsed.substring(1).toLong() * -1)
        } else if (parsed.startsWith("+")) {
            return IntegerRespMessage(parsed.substring(1).toLong())
        }
        return IntegerRespMessage(parsed.toLong())
    }

    private fun parseBulkString(): RespMessage {
        // Redis limits the size of a bulk string to 512MB
        val length = readUntilNewLine().toInt()
        if (length == -1) {
            return NullRespMessage
        }
        if (length == 0) {
            stream.readNBytes(2)
            return BulkStringRespMessage("")
        }
        val parsed = stream.readNBytes(length)
        stream.readNBytes(2)
        return BulkStringRespMessage(String(parsed))
    }

    private fun parseArray(): ArrayRespMessage {
        val length = readUntilNewLine().toUInt()
        val values = mutableListOf<RespMessage>()
        for (i in 0u until length) {
            values.add(parse()!!)
        }
        return ArrayRespMessage(values)
    }

    override fun close() {
        stream.close()
    }
}
