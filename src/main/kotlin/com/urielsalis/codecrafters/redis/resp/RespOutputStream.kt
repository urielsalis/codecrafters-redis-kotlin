package com.urielsalis.codecrafters.redis.resp

import java.io.Closeable
import java.io.OutputStream
import kotlin.math.absoluteValue

class RespOutputStream(val stream: OutputStream) : Closeable {
    fun write(message: RespMessage) {
        when (message) {
            is NullRespMessage -> {
                write("$-1")
                writeCLRF()
            }

            is SimpleStringRespMessage -> {
                write("+")
                write(message.value)
                writeCLRF()
            }

            is ErrorRespMessage -> {
                write("-")
                write(message.value)
                writeCLRF()
            }

            is IntegerRespMessage -> {
                write(":")
                if (message.value < 0) {
                    write("-")
                }
                write(message.value.absoluteValue)
                writeCLRF()
            }

            is BulkStringRespMessage -> {
                write("$")
                write(message.value.length)
                writeCLRF()
                write(message.value)
                writeCLRF()
            }

            is ArrayRespMessage -> {
                write("*")
                write(message.values.size)
                writeCLRF()
                message.values.forEach { write(it) }
            }
        }
    }

    fun write(array: ByteArray) {
        stream.write(array)
    }

    fun write(str: String) {
        write(str.toByteArray())
    }

    fun write(num: Number) {
        write(num.toString())
    }

    fun writeCLRF() {
        write("\r\n")
    }

    fun flush() {
        stream.flush()
    }

    override fun close() {
        stream.close()
    }

}
