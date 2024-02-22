package com.urielsalis.codecrafters.redis

import java.io.IOException
import java.net.ServerSocket

fun main(args: Array<String>) {
    val port = 6379
    val serverSocket = ServerSocket(port)
    serverSocket.setReuseAddress(true)

    try {
        val clientSocket = serverSocket.accept()
    } catch (e: IOException) {
        println("IOException: " + e.message)
    }
}