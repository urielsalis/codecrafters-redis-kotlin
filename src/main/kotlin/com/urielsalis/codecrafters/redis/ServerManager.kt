package com.urielsalis.codecrafters.redis

import java.net.ServerSocket
import kotlin.concurrent.thread

class ServerManager(val serverSocket: ServerSocket) {
    val clients = mutableListOf<Client>()

    fun acceptConnectionsLoop() {
        while (true) {
            val clientSocket = serverSocket.accept()
            val client = Client(clientSocket)
            clients.add(client)
            thread { client.handle() }
        }
    }
}
