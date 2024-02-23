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
        println("Received raw bytes: ${String(bytes.value)}")
        // TODO handle commands from master
    }

    fun initReplication() {
        sendCommand("PING")
        client.readMessage() as SimpleStringRespMessage
        sendCommand("REPLCONF", "listening-port", serverSocket.localPort.toString())
        client.readMessage() as SimpleStringRespMessage
        sendCommand("REPLCONF", "capa", "psync2")
        client.readMessage() as SimpleStringRespMessage
        sendCommand("PSYNC", replId, replOffset.toString())
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
    }

    private fun sendCommand(command: String, vararg args: String) {
        val argsResp = args.map { BulkStringRespMessage(it) }.toTypedArray()
        val message = ArrayRespMessage(listOf(BulkStringRespMessage(command), *argsResp))
        client.sendMessage(message)
    }

    override fun set(key: String, value: RespMessage, expiry: Instant): RespMessage? {
        super.set(key, value, expiry)
        return null
    }

    override fun replicate(command: ArrayRespMessage) {
        // No need to do anything here
    }

    fun replicationLoop() {
        while (true) {
            val message = client.readMessage()
            if (message == null) {
                println("Connection closed")
                return
            }

            if (message is ArrayRespMessage) {
                handleCommand(client, message)
            } else if (message is BulkStringBytesRespMessage) {
                handleRawBytes(client, message)
            }
        }
    }
}