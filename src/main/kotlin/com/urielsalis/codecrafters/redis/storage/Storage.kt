package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.time.Instant

interface Storage {
    fun set(key: String, value: RespMessage, expiry: Instant)
    fun get(key: String): RespMessage?

    fun getConfig(key: String): ArrayRespMessage?
    fun getKeys(pattern: String): ArrayRespMessage
    fun getType(key: String): String
    fun xadd(streamKey: String, entryId: String, arguments: Map<String, String>): RespMessage
    fun xrange(streamKey: String, start: String?, end: String?): RespMessage
}
