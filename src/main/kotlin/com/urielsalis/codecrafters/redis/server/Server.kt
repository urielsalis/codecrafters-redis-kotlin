package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringBytesRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.NullRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket
import java.time.Instant
import kotlin.concurrent.thread

abstract class Server(
    private val serverSocket: ServerSocket,
    protected val storage: Storage,
    initialReplId: String,
    initialReplOffset: Long
) {
    private val clients = mutableListOf<Client>()
    var replId = initialReplId
    var replOffset = initialReplOffset

    fun acceptConnectionsLoop() {
        while (true) {
            val clientSocket = serverSocket.accept()
            val client = Client(clientSocket)
            clients.add(client)
            thread {
                client.handle {
                    println("Message from client: $it")
                    if (it is ArrayRespMessage) {
                        handleCommand(client, it)
                    } else if (it is BulkStringBytesRespMessage) {
                        handleRawBytes(client, it)
                    }
                }
            }
        }
    }

    @Synchronized
    fun handleCommand(client: Client, command: ArrayRespMessage) {
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
                    val message = set(commandArgs[0], BulkStringRespMessage(commandArgs[1]), expiry)
                    if (message != null) {
                        client.sendMessage(message)
                    }
                    replicate(command)
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
                    val messages = mutableMapOf<String, String>()
                    messages["role"] = getRole()
                    messages["master_replid"] = replId
                    messages["master_repl_offset"] = replOffset.toString()
                    messages.map { (key, value) -> "$key:$value" }.joinToString("\r\n")
                        .let { client.sendMessage(BulkStringRespMessage(it)) }
                }
            }

            else -> {
                handleUnknownCommand(client, commandName, commandArgs)
            }
        }
    }

    abstract fun replicate(command: ArrayRespMessage)

    open fun set(key: String, value: RespMessage, expiry: Instant): RespMessage? {
        storage.set(key, value, expiry)
        return null
    }

    abstract fun getRole(): String

    abstract fun handleUnknownCommand(
        client: Client, commandName: String, commandArgs: List<String>
    )

    abstract fun handleRawBytes(client: Client, bytes: BulkStringBytesRespMessage)
}
