package com.urielsalis.codecrafters.redis.resp

sealed class RespMessage

data object NullRespMessage : RespMessage()
data class SimpleStringRespMessage(val value: String) : RespMessage()

data class ErrorRespMessage(val value: String) : RespMessage()
data class IntegerRespMessage(val value: Long) : RespMessage()
data class BulkStringRespMessage(val value: String) : RespMessage()
data class BulkStringBytesRespMessage(val value: ByteArray) : RespMessage()
data class ArrayRespMessage(val values: List<RespMessage>) : RespMessage()