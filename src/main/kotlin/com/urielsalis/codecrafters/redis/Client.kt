package com.urielsalis.codecrafters.redis

import java.net.Socket

class Client(val clientSocket: Socket) {
    fun handle() {
        val input = clientSocket.getInputStream().bufferedReader()
        val output = clientSocket.getOutputStream()
        while (true) {
            val read = input.readLine()
            if (read == null) {
                clientSocket.close()
                return
            }
            if (read.lowercase().equals("ping")) {
                output.write("+PONG\r\n".toByteArray())
            }
        }
    }

}
