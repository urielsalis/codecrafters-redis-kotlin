package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.RespMessage

interface Storage {
    fun set(key: String, value: RespMessage)
    fun get(key: String): RespMessage?
}
