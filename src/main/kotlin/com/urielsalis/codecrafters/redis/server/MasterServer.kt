package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket

class MasterServer(serverSocket: ServerSocket, storage: Storage) :
    Server(serverSocket, storage, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", 0) {
    override fun getRole() = "master"
}