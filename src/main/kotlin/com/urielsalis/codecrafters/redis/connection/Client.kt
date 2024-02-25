package com.urielsalis.codecrafters.redis.connection

import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.io.IOException
import java.net.Socket
import java.net.SocketException

class Client(private val clientSocket: Socket) {
    private val connectionManager = ConnectionManager(clientSocket)
    fun handle(func: (RespMessage) -> Unit) {
        while (true) {
            try {
                val message = readMessage() ?: return
                func(message)
            } catch (e: SocketException) {
                println("Connection closed!")
                return
            }
        }
    }

    fun sendMessage(respMessage: RespMessage) {
        try {
            connectionManager.sendMessage(respMessage)
        } catch (e: SocketException) {
            println("Connection closed")
            connectionManager.close()
        }
    }

    fun readMessage(): RespMessage? {
        val message = try {
            connectionManager.readMessage()
        } catch (e: SocketException) {
            null
        } catch (e: IOException) {
            null
        }
        if (message == null) {
            connectionManager.close()
            return null
        }
        return message
    }

    override fun toString(): String {
        return "Client($clientSocket)"
    }

    override fun equals(other: Any?): Boolean {
        return other is Client &&
            other.clientSocket.port == clientSocket.port &&
            other.clientSocket.inetAddress.address.contentEquals(clientSocket.inetAddress.address)
    }

    override fun hashCode(): Int {
        return clientSocket.port.hashCode() * 31 + clientSocket.inetAddress.hashCode()
    }
}
