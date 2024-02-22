package com.urielsalis.codecrafters.redis

import java.net.Socket

class Client(clientSocket: Socket) {
    private val connectionManager = ConnectionManager(clientSocket)
    fun handle() {
        while (true) {
            val message = connectionManager.readMessage()
            if (message == null) {
                connectionManager.close()
                return
            }
            if (message.lowercase().equals("ping")) {
                connectionManager.sendMessage("+PONG\r\n".toByteArray())
            }
        }
    }

}
