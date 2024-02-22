package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringBytesRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket
import java.net.Socket
import java.time.Instant

class ReplicaServer(
    val serverSocket: ServerSocket, storage: Storage, masterHost: String, masterPort: Int
) : Server(serverSocket, storage, "?", -1) {
    private val client = Client(Socket(masterHost, masterPort))
    override fun getRole() = "slave"
    override fun handleUnknownCommand(
        client: Client,
        commandName: String,
        commandArgs: List<String>
    ) {
        client.sendMessage(ErrorRespMessage("Unknown command: $commandName"))
    }

    override fun handleRawBytes(client: Client, bytes: BulkStringBytesRespMessage) {
        // TODO handle commands from master
    }

    fun replicationLoop() {
        sendCommand("PING")
        val pong = client.readMessage() as SimpleStringRespMessage
        println("Answer to ping: ${pong.value}")
        sendCommand("REPLCONF", "listening-port", serverSocket.localPort.toString())
        val okMsg1 = client.readMessage() as SimpleStringRespMessage
        println("Answer to REPLCONF listening-port: ${okMsg1.value}")
        sendCommand("REPLCONF", "capa", "psync2")
        val okMsg2 = client.readMessage() as SimpleStringRespMessage
        println("Answer to REPLCONF capa: ${okMsg2.value}")
        sendCommand("PSYNC", replId, replOffset.toString())
        val psyncAnswer = client.readMessage()
        println("Answer to PSYNC: $psyncAnswer")
        val firstSync = client.readMessage()
        if (firstSync !is BulkStringBytesRespMessage) {
            println("Expected RDB file, got $firstSync")
        }
        // TODO handle RDB file, for now we only receive empty ones
    }

    private fun sendCommand(command: String, vararg args: String) {
        val argsResp = args.map { BulkStringRespMessage(it) }.toTypedArray()
        val message = ArrayRespMessage(listOf(BulkStringRespMessage(command), *argsResp))
        client.sendMessage(message)
    }

    override fun set(key: String, value: RespMessage, expiry: Instant): RespMessage {
        return ErrorRespMessage("READONLY You can't write against a read only replica.")
    }

    override fun replicate(command: ArrayRespMessage) {
        // No need to do anything here
    }
}