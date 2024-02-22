package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.time.Instant

class InMemoryStorage : Storage {
    val map = mutableMapOf<String, Pair<Instant, RespMessage>>()
    override fun set(key: String, value: RespMessage, expiry: Instant) {
        map[key] = expiry to value
    }

    override fun get(key: String): RespMessage? {
        val pair = map[key] ?: return null
        if (Instant.now().isAfter(pair.first)) {
            map.remove(key)
            return null
        }
        return pair.second
    }

}
