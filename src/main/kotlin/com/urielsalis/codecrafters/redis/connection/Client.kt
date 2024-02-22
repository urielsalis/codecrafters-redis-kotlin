package com.urielsalis.codecrafters.redis.connection

import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.net.Socket
import java.net.SocketException

class Client(clientSocket: Socket) {
    private val connectionManager = ConnectionManager(clientSocket)
    fun handle(func: (RespMessage) -> Unit) {
        while (true) {
            try {
                val message = readMessage() ?: return
                func(message)
            } catch (e: SocketException) {
                println("Connection closed!")
                return
            }
        }
    }

    fun sendMessage(respMessage: RespMessage) {
        connectionManager.sendMessage(respMessage)
    }

    fun readMessage(): RespMessage? {
        val message = connectionManager.readMessage()
        if (message == null) {
            connectionManager.close()
            return null
        }
        return message
    }

}
