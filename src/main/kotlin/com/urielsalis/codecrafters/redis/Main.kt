package com.urielsalis.codecrafters.redis

import java.net.ServerSocket
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    val port = 6379
    val serverSocket = ServerSocket(port)
    serverSocket.setReuseAddress(true)

    val serverManager = ServerManager(serverSocket)
    thread { serverManager.acceptConnectionsLoop() }
}