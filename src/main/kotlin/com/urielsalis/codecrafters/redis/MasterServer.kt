package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket

class MasterServer(serverSocket: ServerSocket, storage: Storage) : Server(serverSocket, storage) {
    override fun getRole() = "master"
}