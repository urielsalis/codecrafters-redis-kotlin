package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.time.Instant

class InMemoryStorage : Storage {
    private val map = mutableMapOf<String, Pair<Instant, RespMessage>>()
    override fun set(key: String, value: RespMessage, expiry: Instant) {
        synchronized(map) {
            map[key] = expiry to value
        }
    }

    override fun get(key: String): RespMessage? {
        synchronized(map) {
            val pair = map[key] ?: return null
            if (Instant.now().isAfter(pair.first)) {
                map.remove(key)
                return null
            }
            return pair.second
        }
    }

    override fun getConfig(key: String): ArrayRespMessage? {
        return null
    }

    override fun getKeys(pattern: String): ArrayRespMessage {
        val keys = if (pattern == "*") {
            map.keys
        } else {
            map.keys.filter { it.matches(pattern.toRegex()) }
        }
        return ArrayRespMessage(keys.map { BulkStringRespMessage(it) })
    }

    override fun getType(key: String): String {
        return when (val value = get(key)) {
            is BulkStringRespMessage -> "string"
            is ArrayRespMessage -> "list"
            null -> "none"
            else -> throw IllegalArgumentException("Unsupported type " + value::class.simpleName)
        }
    }

}
