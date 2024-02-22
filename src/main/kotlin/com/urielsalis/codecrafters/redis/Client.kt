package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
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
            val command = message as ArrayRespMessage
            val commandName = (command.values[0] as BulkStringRespMessage).value.lowercase()
            val commandArgs = if (command.values.size == 1) {
                emptyList()
            } else {
                command.values.subList(1, command.values.size)
                    .map { (it as BulkStringRespMessage).value }
            }

            when (commandName) {
                "ping" -> {
                    connectionManager.sendMessage(SimpleStringRespMessage("PONG"))
                }

                "echo" -> {
                    connectionManager.sendMessage(SimpleStringRespMessage(commandArgs.joinToString(" ")))
                }
            }
        }
    }

}
