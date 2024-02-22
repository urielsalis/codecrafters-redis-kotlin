package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket
import java.net.Socket

class ReplicaServer(
    serverSocket: ServerSocket, storage: Storage, masterHost: String, masterPort: Int
) : Server(serverSocket, storage, "?", -1) {
    private val client = Client(Socket(masterHost, masterPort))
    override fun getRole() = "slave"
    fun replicationLoop() {
        sendCommand("PING")
        val pong = client.readMessage() as SimpleStringRespMessage
        println("Answer to ping: ${pong.value}")
    }

    private fun sendCommand(command: String, vararg args: String) {
        val argsResp = args.map { BulkStringRespMessage(it) }.toTypedArray()
        val message = ArrayRespMessage(listOf(BulkStringRespMessage(command), *argsResp))
        client.sendMessage(message)
    }
}