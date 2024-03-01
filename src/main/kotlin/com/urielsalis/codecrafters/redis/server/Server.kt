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
                if (commandArgs.size < 2) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'xadd' command"))
                } else {
                    val streamKey = commandArgs[0]
                    val entryId = commandArgs[1]
                    val arguments = commandArgs.subList(2, commandArgs.size).chunked(2)
                        .associate { it[0] to it[1] }
                    client.sendMessage(storage.xadd(streamKey, entryId, arguments))
                }
            }

            "xrange" -> {
                if (commandArgs.isEmpty()) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'xrange' command"))
                } else {
                    val streamKey = commandArgs[0]
                    val start = commandArgs.getOrNull(1)
                    val end = commandArgs.getOrNull(2)
                    val message = storage.xrange(streamKey, start, end)
                    client.sendMessage(message)
                }
            }

            "xread" -> {
                if (commandArgs.size < 3) {
                    client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'xread' command"))
                } else {
                    val blockTime = if (commandArgs.contains("block")) {
                        val value = commandArgs[commandArgs.indexOf("block") + 1].toLong()
                        if (value > 0) {
                            value
                        } else {
                            TODO()
                            // return max :D
                        }
                    } else {
                        -1L
                    }
                    val start = commandArgs.indexOf("streams")
                    if (start == -1) {
                        client.sendMessage(ErrorRespMessage("Invalid arguments for 'xread' command"))
                    } else {
                        val streamsAndKeys = commandArgs.subList(start + 1, commandArgs.size)
                        val streams = streamsAndKeys.subList(0, streamsAndKeys.size / 2)
                        val keys =
                            streamsAndKeys.subList(streamsAndKeys.size / 2, streamsAndKeys.size)
                        val streamToKeys = streams.zip(keys)

                        if (blockTime == -1L) {
                            val message = ArrayRespMessage(streamToKeys.map { (stream, key) ->
                                storage.xread(stream, key)
                            })
                            client.sendMessage(message)
                        } else {
                            val initialMaxKeys = streamToKeys.map { (stream, keys) ->
                                stream to keys
                            }
                            var currentTime = Instant.now().toEpochMilli()
                            val maxTime = currentTime + blockTime
                            var sent = false
                            while (currentTime <= maxTime) {
                                val message = ArrayRespMessage(initialMaxKeys.map { (stream, key) ->
                                    storage.xread(stream, key)
                                })
                                if (message.values.all {
                                        if (it !is ArrayRespMessage) {
                                            return@all false
                                        }
                                        val last = message.values.lastOrNull()
                                        if (last !is ArrayRespMessage) {
                                            return@all false
                                        }
                                        val last2 = last.values.last()
                                        if (last2 !is ArrayRespMessage) {
                                            return@all false
                                        }
                                        last2.values.isNotEmpty()
                                    }) {
                                    sent = true
                                    client.sendMessage(message)
                                    break
                                }
                                currentTime = Instant.now().toEpochMilli()
                            }
                            if (!sent) {
                                client.sendMessage(NullRespMessage)
                            }
                        }

                    }
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
