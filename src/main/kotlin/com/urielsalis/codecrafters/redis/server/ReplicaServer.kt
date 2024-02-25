package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringBytesRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.RDBStorage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket
import java.net.Socket

class ReplicaServer(
    val serverSocket: ServerSocket, storage: Storage, masterHost: String, masterPort: Int
) : Server(serverSocket, storage, "?", -1) {
    private val client = Client(Socket(masterHost, masterPort))
    override fun getRole() = "slave"

    override fun handleUnknownCommand(
        client: Client, commandName: String, commandArgs: List<String>
    ) {
        client.sendMessage(ErrorRespMessage("Unknown command: $commandName"))
    }

    fun handleMasterCommand(commandName: String, commandArgs: List<String>) {
        when (commandName) {
            "replconf" -> {
                if (commandArgs.size != 2 || commandArgs[0].lowercase() != "getack" || commandArgs[1] != "*") {
                    client.sendMessage(
                        ErrorRespMessage(
                            "Invalid REPLCONF ${
                                commandArgs.joinToString(
                                    " "
                                )
                            }"
                        )
                    )
                    return
                }
                println("Returning REPLCONF ACK $replOffset")
                client.sendMessage(getCommand("REPLCONF", "ACK", replOffset.toString()))
            }

            "set" -> handleSet(commandArgs)

            "ping" -> println("Received ping from master")

            else -> client.sendMessage(ErrorRespMessage("Unknown command: $commandName"))
        }
    }

    fun initReplication() {
        client.sendMessage(getCommand("PING"))
        client.readMessage() as SimpleStringRespMessage
        client.sendMessage(
            getCommand(
                "REPLCONF", "listening-port", serverSocket.localPort.toString()
            )
        )
        client.readMessage() as SimpleStringRespMessage
        client.sendMessage(getCommand("REPLCONF", "capa", "psync2"))
        client.readMessage() as SimpleStringRespMessage
        client.sendMessage(getCommand("PSYNC", replId, replOffset.toString()))
        val psyncAnswer = client.readMessage()
        val psyncParts = (psyncAnswer as SimpleStringRespMessage).value.split(" ")
        replId = psyncParts[1]
        replOffset = psyncParts[2].toLong()
        println("Master is now at $replId:$replOffset")
        val firstSync = client.readMessage()
        if (firstSync !is BulkStringBytesRespMessage) {
            println("Expected RDB file, got $firstSync")
        } else {
            handleRdbFile(firstSync)
        }
        // TODO handle RDB file, for now we only receive empty ones
    }

    private fun handleRdbFile(firstSync: BulkStringBytesRespMessage) {
        println("Received RDB file")
        storage = RDBStorage(bytes = firstSync.value)
    }

    override fun handleWriteCommand(client: Client, command: ArrayRespMessage) {
        println("Write commands are not allowed on replicas, should never happen")
    }


    fun replicationLoop() {
        while (true) {
            val message = client.readMessage()
            if (message == null) {
                println("Connection closed")
                return
            }

            if (message is ArrayRespMessage) {
                val (commandName, commandArgs) = parseCommand(message)
                handleMasterCommand(commandName, commandArgs)
                replOffset += encodedSize(message)
            } else {
                println("Unknown message type: $message")
            }
        }
    }

}