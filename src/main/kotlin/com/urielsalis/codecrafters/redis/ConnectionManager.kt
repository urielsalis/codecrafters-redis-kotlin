package com.urielsalis.codecrafters.redis

import java.io.BufferedReader
import java.io.Closeable
import java.io.OutputStream
import java.net.Socket

class ConnectionManager(val socket: Socket) : Closeable {
    private val input: BufferedReader = socket.getInputStream().bufferedReader()
    private val output: OutputStream = socket.getOutputStream()

    fun readMessage(): String? {
        return input.readLine()
    }

    fun sendMessage(message: ByteArray) {
        output.write(message)
        output.flush()
    }

    override fun close() {
        input.close()
        output.close()
        socket.close()
    }
}