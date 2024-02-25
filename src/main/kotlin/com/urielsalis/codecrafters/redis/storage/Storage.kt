package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.time.Instant

interface Storage {
    fun set(key: String, value: RespMessage, expiry: Instant)
    fun get(key: String): RespMessage?

    fun getConfig(key: String): ArrayRespMessage?
}
