package com.urielsalis.codecrafters.redis

import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket

class ReplicaServer(
    serverSocket: ServerSocket, storage: Storage, masterHost: String, masterPort: Int
) : Server(serverSocket, storage) {
    override fun getRole() = "slave"
}