package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.NullRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket
import java.time.Instant
import kotlin.concurrent.thread

abstract class Server(private val serverSocket: ServerSocket, private val storage: Storage) {
    private val clients = mutableListOf<Client>()

    fun acceptConnectionsLoop() {
        while (true) {
            val clientSocket = serverSocket.accept()
            val client = Client(clientSocket)
            clients.add(client)
            thread {
                client.handle {
                    handleCommand(client, it)
                }
            }
        }
    }

    private fun handleCommand(client: Client, message: RespMessage) {
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
                client.sendMessage(SimpleStringRespMessage("PONG"))
            }

            "echo" -> {
                client.sendMessage(SimpleStringRespMessage(commandArgs.joinToString(" ")))
            }

            "set" -> {
                if (commandArgs.size < 2) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'set' command"))
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
                    storage.set(commandArgs[0], BulkStringRespMessage(commandArgs[1]), expiry)
                    client.sendMessage(SimpleStringRespMessage("OK"))
                }
            }

            "get" -> {
                if (commandArgs.size != 1) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'get' command"))
                } else {
                    val value = storage.get(commandArgs[0])
                    if (value == null) {
                        client.sendMessage(NullRespMessage)
                    } else {
                        client.sendMessage(value)
                    }
                }
            }

            "info" -> {
                if (commandArgs.size != 1 || commandArgs[0] != "replication") {
                    client.sendMessage(ErrorRespMessage("Unsupported info command"))
                } else {
                    client.sendMessage(SimpleStringRespMessage("role:${getRole()}"))
                }
            }
        }
    }

    abstract fun getRole(): String
}
