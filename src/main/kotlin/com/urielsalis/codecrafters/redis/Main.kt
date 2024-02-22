package com.urielsalis.codecrafters.redis

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.pair
import com.github.ajalt.clikt.parameters.types.int
import com.urielsalis.codecrafters.redis.server.MasterServer
import com.urielsalis.codecrafters.redis.server.ReplicaServer
import com.urielsalis.codecrafters.redis.storage.InMemoryStorage
import java.net.ServerSocket
import kotlin.concurrent.thread


class RedisServer : CliktCommand() {
    private val port by option("-p", "--port", help = "Port to listen to").int().default(6379)
    private val replicaOf: Pair<String, String>? by option(
        "--replicaof",
        help = "Replicate to another server"
    ).pair()
    override fun run() {
        val serverSocket = ServerSocket(port)
        serverSocket.setReuseAddress(true)

        val storage = InMemoryStorage()

        val server = if (replicaOf == null) {
            MasterServer(serverSocket, storage)
        } else {
            val server =
                ReplicaServer(serverSocket, storage, replicaOf!!.first, replicaOf!!.second.toInt())
            thread { server.replicationLoop() }
            server
        }
        thread { server.acceptConnectionsLoop() }
    }
}

fun main(args: Array<String>) = RedisServer().main(args)