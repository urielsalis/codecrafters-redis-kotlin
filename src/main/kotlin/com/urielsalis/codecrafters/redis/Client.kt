package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.NullRespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import java.net.Socket
import java.time.Instant

class Client(private val server: Server, clientSocket: Socket) {
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

                "set" -> {
                    if (commandArgs.size < 2) {
                        connectionManager.sendMessage(ErrorRespMessage("Wrong number of arguments for 'set' command"))
                    } else {
                        val expiry = if (commandArgs.size > 2) {
                            if (commandArgs[2].lowercase() == "ex") {
                                Instant.now().plusSeconds(commandArgs[3].toLong())
                            } else if (commandArgs[2].lowercase() == "px") {
                                Instant.now().plusMillis(commandArgs[3].toLong())
                            } else {
                                throw IllegalArgumentException("Invalid expiration " + commandArgs.joinToString { " " })
                            }
                        } else {
                            Instant.MAX
                        }
                        server.set(commandArgs[0], BulkStringRespMessage(commandArgs[1]), expiry)
                        connectionManager.sendMessage(SimpleStringRespMessage("OK"))
                    }
                }

                "get" -> {
                    if (commandArgs.size != 1) {
                        connectionManager.sendMessage(ErrorRespMessage("Wrong number of arguments for 'get' command"))
                    } else {
                        val value = server.get(commandArgs[0])
                        if (value == null) {
                            connectionManager.sendMessage(NullRespMessage)
                        } else {
                            connectionManager.sendMessage(value)
                        }
                    }
                }
            }
        }
    }

}
