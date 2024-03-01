package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.ErrorRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import com.urielsalis.codecrafters.redis.resp.StreamEntry
import com.urielsalis.codecrafters.redis.resp.StreamEntryId
import com.urielsalis.codecrafters.redis.resp.StreamRespMessage
import java.time.Instant
import kotlin.math.max

open class InMemoryStorage : Storage {
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
            is StreamRespMessage -> "stream"
            null -> "none"
            else -> throw IllegalArgumentException("Unsupported type " + value::class.simpleName)
        }
    }

    override fun xadd(
        streamKey: String, entryId: String, arguments: Map<String, String>
    ): RespMessage {
        synchronized(map) {
            val entryIdObj = getEntry(streamKey, entryId, arguments)
            if (entryIdObj.first is StreamRespMessage) {
                set(streamKey, entryIdObj.first, Instant.MAX)
                return entryIdObj.second!!.let { BulkStringRespMessage("${it.ms}-${it.seq}") }
            }
            return entryIdObj.first
        }
    }

    private fun getEntry(
        streamKey: String, entryId: String, arguments: Map<String, String>
    ): Pair<RespMessage, StreamEntryId?> {
        val entryIdParts = entryId.split("-")
        if (entryIdParts.size != 2 && entryId != "*") {
            return ErrorRespMessage("Invalid stream ID") to null
        }
        val (idMs, idSeq) = if (entryId == "*") {
            null to null
        } else {
            try {
                longOrNull(entryIdParts[0]) to longOrNull(entryIdParts[1])
            } catch (e: IllegalArgumentException) {
                return ErrorRespMessage("Invalid stream ID") to null
            }
        }

        // Validate we are greater than 0-0
        if ((idMs != null && idMs < 0) || (idSeq != null && idSeq < 0)) {
            return ErrorRespMessage("ERR The ID specified in XADD must be greater than 0-0") to null
        }
        if (idMs == null && idSeq != null) {
            return ErrorRespMessage("ERR The ID specified in XADD must be greater than 0-0") to null
        }
        if (idMs == 0L && idSeq == 0L) {
            return ErrorRespMessage("ERR The ID specified in XADD must be greater than 0-0") to null
        }

        // Validate that we fit!
        val currentStream = get(streamKey)
        if (currentStream != null && currentStream !is StreamRespMessage) {
            return ErrorRespMessage("WRONGTYPE Operation against a key holding the wrong kind of value") to null
        }
        //     No current stream, so we do!
        if (currentStream == null) {
            val id = generateId(idMs, idSeq, 0)
            return StreamRespMessage(streamKey, mutableListOf(StreamEntry(id, arguments))) to id
        }
        //     Current stream, so we ddo need to check
        val existingStream = currentStream as StreamRespMessage
        val maxMs = existingStream.values.maxOf { it.id.ms }
        val maxSeq = if (idMs != null) {
            existingStream.values.filter { it.id.ms == idMs }.maxOfOrNull { it.id.seq } ?: -1
        } else {
            existingStream.values.filter { it.id.ms == maxMs }.maxOf { it.id.seq }
        }
        val id = generateId(idMs, idSeq, maxSeq + 1)
        if (id.ms < maxMs) {
            return ErrorRespMessage("ERR The ID specified in XADD is equal or smaller than the target stream top item") to null
        }
        if (id.ms == maxMs && id.seq <= maxSeq) {
            return ErrorRespMessage("ERR The ID specified in XADD is equal or smaller than the target stream top item") to null
        }
        // Add it!
        existingStream.values.add(StreamEntry(id, arguments))
        return existingStream to id
    }

    private fun generateId(idMs: Long?, idSeq: Long?, minimumSeq: Long): StreamEntryId {
        if (idMs == null) {
            TODO()
        }
        if (idSeq == null) {
            if (idMs == 0L) {
                return StreamEntryId(0, max(minimumSeq, 1))
            }
            return StreamEntryId(idMs, minimumSeq)
        }
        return StreamEntryId(idMs, idSeq)
    }

    private fun longOrNull(str: String): Long? {
        if (str == "*") {
            return null
        }
        return try {
            str.toLong()
        } catch (e: NumberFormatException) {
            throw IllegalArgumentException("Invalid long: $str")
        }
    }

}
