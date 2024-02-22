package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket

class MasterServer(serverSocket: ServerSocket, storage: Storage) :
    Server(serverSocket, storage, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0) {

    override fun getRole() = "master"
    override fun handleUnknownCommand(
        client: Client, commandName: String, commandArgs: List<String>
    ) {
        when (commandName) {
            "replconf" -> {
                client.sendMessage(SimpleStringRespMessage("OK"))
            }

            else -> {
                client.sendMessage(ErrorRespMessage("Unknown command: $commandName"))
            }
        }
    }
}