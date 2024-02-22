package com.urielsalis.codecrafters.redis.connection

import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.net.Socket

class Client(clientSocket: Socket) {
    private val connectionManager = ConnectionManager(clientSocket)
    fun handle(func: (RespMessage) -> Unit) {
        while (true) {
            val message = connectionManager.readMessage()
            if (message == null) {
                connectionManager.close()
                return
            }
            func(message)
        }
    }

    fun sendMessage(simpleStringRespMessage: RespMessage) {
        connectionManager.sendMessage(simpleStringRespMessage)
    }

}
