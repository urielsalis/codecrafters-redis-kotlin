package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.RespMessage

class InMemoryStorage : Storage {
    val map = mutableMapOf<String, RespMessage>()
    override fun set(key: String, value: RespMessage) {
        map[key] = value
    }

    override fun get(key: String): RespMessage? = map[key]

}
