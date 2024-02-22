package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.resp.RespInputStream
import com.urielsalis.codecrafters.redis.resp.RespMessage
import com.urielsalis.codecrafters.redis.resp.RespOutputStream
import java.io.Closeable
import java.net.Socket

class ConnectionManager(val socket: Socket) : Closeable {
    private val input: RespInputStream = RespInputStream(socket.getInputStream())
    private val output: RespOutputStream = RespOutputStream(socket.getOutputStream())

    fun readMessage(): RespMessage? {
        return input.read()
    }

    fun sendMessage(message: RespMessage) {
        output.write(message)
        output.flush()
    }

    override fun close() {
        input.close()
        output.close()
        socket.close()
    }
}