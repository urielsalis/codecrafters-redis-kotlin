package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.NullRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
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
                    return client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'set' command"))
                }
                handleSet(commandArgs)
                handleWriteCommand(client, command)
            }

            "get" -> {
                if (commandArgs.size != 1) {
                    return client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'get' command"))
                }
                val value =
                    storage.get(commandArgs[0]) ?: return client.sendMessage(NullRespMessage)
                client.sendMessage(value)
            }

            "info" -> {
                if (commandArgs.size != 1 || commandArgs[0] != "replication") {
                    return client.sendMessage(ErrorRespMessage("Unsupported info command"))
                }
                val messages = mutableMapOf<String, String>()
                messages["role"] = getRole()
                messages["master_replid"] = replId
                messages["master_repl_offset"] = replOffset.toString()
                messages.map { (key, value) -> "$key:$value" }.joinToString("\r\n")
                    .let { client.sendMessage(BulkStringRespMessage(it)) }
            }

            "config" -> {
                if (commandArgs.size != 2 || commandArgs[0].lowercase() != "get") {
                    return client.sendMessage(ErrorRespMessage("Unsupported config command"))
                }
                val value =
                    storage.getConfig(commandArgs[1]) ?: return client.sendMessage(NullRespMessage)
                client.sendMessage(value)
            }

            "keys" -> {
                if (commandArgs.size != 1) {
                    return client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'keys' command"))
                }
                client.sendMessage(storage.getKeys(commandArgs[0]))
            }

            "type" -> {
                if (commandArgs.size != 1) {
                    return client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'type' command"))
                }
                client.sendMessage(SimpleStringRespMessage(storage.getType(commandArgs[0])))
            }

            "xadd" -> {
                if (commandArgs.size < 2) {
                    return client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'xadd' command"))
                }
                val streamKey = commandArgs[0]
                val entryId = commandArgs[1]
                val arguments =
                    commandArgs.subList(2, commandArgs.size).chunked(2).associate { it[0] to it[1] }
                client.sendMessage(storage.xadd(streamKey, entryId, arguments))
            }

            "xrange" -> {
                if (commandArgs.isEmpty()) {
                    return client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'xrange' command"))
                }
                val streamKey = commandArgs[0]
                val start = commandArgs.getOrNull(1)
                val end = commandArgs.getOrNull(2)
                val message = storage.xrange(streamKey, start, end)
                client.sendMessage(message)
            }

            "xread" -> {
                if (commandArgs.size < 3) {
                    return client.sendMessage(ErrorRespMessage("Wrong number of arguments for 'xread' command"))
                }
                val blockTime = getBlockTime(commandArgs)
                val start = commandArgs.indexOf("streams")
                if (start == -1) {
                    return client.sendMessage(ErrorRespMessage("Invalid arguments for 'xread' command"))
                }
                val streamToKeys = zipStreamToKeys(commandArgs, start)

                if (blockTime == -1L) {
                    val message = ArrayRespMessage(streamToKeys.map { (stream, key) ->
                        storage.xread(stream, key)
                    })
                    return client.sendMessage(message)
                }

                val initialMaxKeys = streamToKeys.map { (stream, keys) ->
                    stream to keys
                }
                client.sendMessage(handlexreadBlocking(blockTime, initialMaxKeys))
            }

            else -> handleUnknownCommand(client, commandName, commandArgs)
        }
    }

    private fun handlexreadBlocking(
        blockTime: Long, initialMaxKeys: List<Pair<String, String>>
    ): RespMessage {
        var currentTime = Instant.now().toEpochMilli()
        val maxTime = if (blockTime == Long.MAX_VALUE) {
            Long.MAX_VALUE
        } else {
            currentTime + blockTime
        }
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
                return message
            }
            currentTime = Instant.now().toEpochMilli()
        }
        return NullRespMessage
    }

    private fun zipStreamToKeys(
        commandArgs: List<String>, start: Int
    ): List<Pair<String, String>> {
        val streamsAndKeys = commandArgs.subList(start + 1, commandArgs.size)
        val streams = streamsAndKeys.subList(0, streamsAndKeys.size / 2)
        val keys = streamsAndKeys.subList(streamsAndKeys.size / 2, streamsAndKeys.size)
        val streamToKeys = streams.zip(keys)
        return streamToKeys
    }

    private fun getBlockTime(commandArgs: List<String>) = if (commandArgs.contains("block")) {
        val value = commandArgs[commandArgs.indexOf("block") + 1].toLong()
        if (value > 0) {
            value
        } else {
            Long.MAX_VALUE
        }
    } else {
        -1L
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
