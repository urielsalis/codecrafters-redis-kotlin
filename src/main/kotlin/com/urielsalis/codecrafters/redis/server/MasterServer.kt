package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.connection.Client
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringBytesRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.IntegerRespMessage
import com.urielsalis.codecrafters.redis.resp.SimpleStringRespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.io.File
import java.net.ServerSocket
import java.time.Instant
import kotlin.concurrent.thread

class MasterServer(serverSocket: ServerSocket, storage: Storage) :
    Server(serverSocket, storage, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0) {

    private val replicas = mutableMapOf<Client, Long>()
    override fun getRole() = "master"
    override fun handleUnknownCommand(
        client: Client, commandName: String, commandArgs: List<String>
    ) {
        when (commandName) {
            "replconf" -> {
                if (commandArgs.size == 2 && commandArgs[0].lowercase() == "ack") {
                    println("Setting ack of $client to ${commandArgs[1]} at ${Instant.now()}")
                    synchronized(replicas) {
                        replicas[client] = commandArgs[1].toLong()
                    }
                } else {
                    client.sendMessage(SimpleStringRespMessage("OK"))
                }
            }

            "psync" -> {
                client.sendMessage(SimpleStringRespMessage("FULLRESYNC $replId $replOffset"))
                client.sendMessage(BulkStringBytesRespMessage(File("empty.rdb").readBytes()))
                synchronized(replicas) {
                    replicas[client] = 0
                }
            }

            "wait" -> {
                val expectedReplicas = commandArgs[0].toInt()
                val timeout = commandArgs[1].toLong()
                val currentOffset = replOffset
                val lock = Any()
                var ackedPreviousCommand = 0L

                if (currentOffset == 0L) {
                    client.sendMessage(IntegerRespMessage(replicas.size.toLong()))
                    return
                }

                var runThreads = true
                replicas.map { (replica, previousOffset) ->
                    thread {
                        while (runThreads && ackedPreviousCommand < expectedReplicas) {
                            val command = getCommand("REPLCONF", "GETACK", "*")
                            replica.sendMessage(command)
                            replOffset += encodedSize(command)
                            while (runThreads && replicas[replica] == previousOffset) {
                                Thread.yield()
                            }
                            val newOffset = replicas[replica]!!
                            if (newOffset >= currentOffset) {
                                println("Replica $replica has offset $newOffset")
                                synchronized(lock) {
                                    ackedPreviousCommand++
                                }
                                break
                            } else {
                                println("Replica $replica is behind with offset $newOffset, waiting for $currentOffset")
                            }
                        }
                    }
                }
                val timeoutTime = Instant.now().plusMillis(timeout)
                while (timeoutTime.isAfter(Instant.now()) && ackedPreviousCommand < expectedReplicas) {
                    Thread.sleep(100)
                }
                runThreads = false
                client.sendMessage(IntegerRespMessage(ackedPreviousCommand))
            }

            else -> {
                client.sendMessage(ErrorRespMessage("Unknown command: $commandName"))
            }
        }
    }

    override fun handleWriteCommand(client: Client, command: ArrayRespMessage) {
        replOffset += encodedSize(command)
        replicas.forEach { it.key.sendMessage(command) }
        client.sendMessage(SimpleStringRespMessage("OK"))
    }
}