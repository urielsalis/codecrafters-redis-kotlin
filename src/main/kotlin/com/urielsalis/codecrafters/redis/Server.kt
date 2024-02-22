package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.resp.RespMessage
import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket
import kotlin.concurrent.thread

class Server(private val serverSocket: ServerSocket, private val storage: Storage) {
    private val clients = mutableListOf<Client>()

    fun acceptConnectionsLoop() {
        while (true) {
            val clientSocket = serverSocket.accept()
            val client = Client(this, clientSocket)
            clients.add(client)
            thread { client.handle() }
        }
    }

    fun set(key: String, value: RespMessage) {
        storage.set(key, value)
    }

    fun get(key: String) = storage.get(key)
}
