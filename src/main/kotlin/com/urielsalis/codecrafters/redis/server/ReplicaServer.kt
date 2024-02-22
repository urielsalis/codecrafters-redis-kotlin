package com.urielsalis.codecrafters.redis.server

import com.urielsalis.codecrafters.redis.storage.Storage
import java.net.ServerSocket

class ReplicaServer(
    serverSocket: ServerSocket, storage: Storage, masterHost: String, masterPort: Int
) : Server(serverSocket, storage, "?", -1) {
    override fun getRole() = "slave"
}