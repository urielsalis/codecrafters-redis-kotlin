package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.NullRespMessage
import com.urielsalis.codecrafters.redis.resp.RespOutputStream
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.io.ByteArrayOutputStream
import java.net.ServerSocket
import java.time.Instant
import kotlin.concurrent.thread

abstract class Server(
    private val serverSocket: ServerSocket,
    protected var storage: Storage,
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
                    if (it is ArrayRespMessage) {
                        handleCommand(client, it)
                    } else {
                        println("Unknown message type: $it")
                    }
                }
            }
        }
    }

    fun handleCommand(client: Client, command: ArrayRespMessage) {
        val (commandName, commandArgs) = parseCommand(command)

        when (commandName) {
            "ping" -> client.sendMessage(SimpleStringRespMessage("PONG"))
            "echo" -> client.sendMessage(SimpleStringRespMessage(commandArgs.joinToString(" ")))
            "set" -> {
                if (commandArgs.size < 2) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'set' command"))
                } else {
                    handleSet(commandArgs)
                    handleWriteCommand(client, command)
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

            "config" -> {
                if (commandArgs.size != 2 || commandArgs[0].lowercase() != "get") {
                    client.sendMessage(ErrorRespMessage("Unsupported config command"))
                } else {
                    val value = storage.getConfig(commandArgs[1])
                    if (value == null) {
                        client.sendMessage(NullRespMessage)
                    } else {
                        client.sendMessage(value)
                    }
                }
            }

            "keys" -> {
                if (commandArgs.size != 1) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'keys' command"))
                } else {
                    val keys = storage.getKeys(commandArgs[0])
                    client.sendMessage(keys)
                }
            }

            "type" -> {
                if (commandArgs.size != 1) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'type' command"))
                } else {
                    val type = storage.getType(commandArgs[0])
                    client.sendMessage(SimpleStringRespMessage(type))
                }
            }

            "xadd" -> {
                if (commandArgs.isEmpty()) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'xadd' command"))
                } else {
                    val streamKey = commandArgs[0]
                    val arguments =
                        commandArgs.subList(1, commandArgs.size - 1).chunked(2)
                            .associate { it[0] to it[1] }

                    val id = storage.xadd(streamKey, arguments)
                    client.sendMessage(BulkStringRespMessage(id))
                }

            }

            else -> handleUnknownCommand(client, commandName, commandArgs)
        }
    }

    protected fun parseCommand(command: ArrayRespMessage): Pair<String, List<String>> {
        val commandName = (command.values[0] as BulkStringRespMessage).value.lowercase()
        val commandArgs = if (command.values.size == 1) {
            emptyList()
        } else {
            command.values.subList(1, command.values.size)
                .map { (it as BulkStringRespMessage).value }
        }
        return Pair(commandName, commandArgs)
    }

    abstract fun handleWriteCommand(client: Client, command: ArrayRespMessage)
    protected fun handleSet(commandArgs: List<String>) {
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
    }

    abstract fun getRole(): String

    abstract fun handleUnknownCommand(
        client: Client, commandName: String, commandArgs: List<String>
    )

    protected fun encodedSize(command: ArrayRespMessage): Int {
        val output = ByteArrayOutputStream()
        val stream = RespOutputStream(output)
        stream.write(command)
        stream.flush()
        val size = output.size()
        stream.close()
        return size
    }

    protected fun getCommand(command: String, vararg args: String): ArrayRespMessage {
        val argsResp = args.map { BulkStringRespMessage(it) }.toTypedArray()
        return ArrayRespMessage(listOf(BulkStringRespMessage(command), *argsResp))
    }

}
