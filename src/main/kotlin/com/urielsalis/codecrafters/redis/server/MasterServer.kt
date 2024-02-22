package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringBytesRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.io.File
import java.net.ServerSocket
import java.time.Instant

class MasterServer(serverSocket: ServerSocket, storage: Storage) :
    Server(serverSocket, storage, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0) {

    val replicas = mutableListOf<Client>()

    override fun getRole() = "master"
    override fun handleUnknownCommand(
        client: Client, commandName: String, commandArgs: List<String>
    ) {
        when (commandName) {
            "replconf" -> {
                client.sendMessage(SimpleStringRespMessage("OK"))
            }

            "psync" -> {
                client.sendMessage(SimpleStringRespMessage("FULLRESYNC $replId $replOffset"))
                client.sendMessage(BulkStringBytesRespMessage(File("empty.rdb").readBytes()))
                replicas.add(client)
            }

            else -> {
                client.sendMessage(ErrorRespMessage("Unknown command: $commandName"))
            }
        }
    }

    override fun handleRawBytes(client: Client, bytes: BulkStringBytesRespMessage) {
        client.sendMessage(ErrorRespMessage("Unknown command"))
    }

    override fun set(key: String, value: RespMessage, expiry: Instant): RespMessage {
        storage.set(key, value, expiry)
        return SimpleStringRespMessage("OK")
    }

    override fun replicate(command: ArrayRespMessage) {
    }
}