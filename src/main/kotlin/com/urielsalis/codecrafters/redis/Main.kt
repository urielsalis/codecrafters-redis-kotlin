package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.storage.InMemoryStorage
import java.net.ServerSocket
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    val port = 6379
    val serverSocket = ServerSocket(port)
    serverSocket.setReuseAddress(true)

    val storage = InMemoryStorage()

    val serverManager = Server(serverSocket, storage)
    thread { serverManager.acceptConnectionsLoop() }
}