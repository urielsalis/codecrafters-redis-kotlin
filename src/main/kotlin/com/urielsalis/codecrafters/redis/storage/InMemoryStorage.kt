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

    override fun xrange(streamKey: String, start: String?, end: String?): RespMessage {
        val stream = get(streamKey) ?: return ArrayRespMessage(emptyList())
        if (stream !is StreamRespMessage) {
            return ErrorRespMessage("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        val minId = if (start == null || start == "-") {
            StreamEntryId(0, 0)
        } else {
            val parts = start.split("-")
            if (parts.size != 2) {
                StreamEntryId(start.toLong(), 0)
            }
            StreamEntryId(parts[0].toLong(), parts[1].toLong())
        }
        val maxId = if (end == null || end == "+") {
            StreamEntryId(Long.MAX_VALUE, Long.MAX_VALUE)
        } else {
            val parts = end.split("-")
            if (parts.size != 2) {
                StreamEntryId(end.toLong(), Long.MAX_VALUE)
            }
            StreamEntryId(parts[0].toLong(), parts[1].toLong())
        }
        val entries = stream.values.filter { it.id.ms >= minId.ms && it.id.seq >= minId.seq }
            .filter { it.id.ms <= maxId.ms && it.id.seq <= maxId.seq }
        return ArrayRespMessage(entries.map {
            ArrayRespMessage(
                listOf(
                    BulkStringRespMessage("${it.id.ms}-${it.id.seq}"),
                    ArrayRespMessage(it.values.flatMap { (k, v) ->
                        listOf(
                            BulkStringRespMessage(k), BulkStringRespMessage(v)
                        )
                    })
                )
            )
        })
    }

    override fun xread(streamKey: String, minId: String): RespMessage {
        val stream = get(streamKey) ?: return ArrayRespMessage(emptyList())
        if (stream !is StreamRespMessage) {
            return ErrorRespMessage("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        val minId = if (minId == "-") {
            StreamEntryId(0, 0)
        } else {
            val parts = minId.split("-")
            if (parts.size != 2) {
                StreamEntryId(minId.toLong(), 1)
            }
            StreamEntryId(parts[0].toLong(), parts[1].toLong() + 1)
        }
        val entries = stream.values.filter { it.id.ms >= minId.ms && it.id.seq >= minId.seq }
        return ArrayRespMessage(
            listOf(
                BulkStringRespMessage(streamKey),
                ArrayRespMessage(entries.map {
                    ArrayRespMessage(
                        listOf(
                            BulkStringRespMessage("${it.id.ms}-${it.id.seq}"),
                            ArrayRespMessage(it.values.flatMap { (k, v) ->
                                listOf(
                                    BulkStringRespMessage(k), BulkStringRespMessage(v)
                                )
                            })
                        )
                    )
                })
            )
        )
    }

    private fun getEntry(
        streamKey: String, entryId: String, arguments: Map<String, String>
    ): Pair<RespMessage, StreamEntryId?> {
        val entryIdParts = entryId.split("-")
        if (entryIdParts.size != 2 && entryId != "*") {
            return ErrorRespMessage("Invalid stream ID") to null
        }
        val (idMs, idSeq) = if (entryId == "*") {
            Instant.now().toEpochMilli() to null
        } else {
            try {
                longOr(entryIdParts[0], Instant.now().toEpochMilli())!! to longOr(
                    entryIdParts[1], null
                )
            } catch (e: IllegalArgumentException) {
                return ErrorRespMessage("Invalid stream ID") to null
            }
        }

        // Validate we are greater than 0-0
        if (idMs < 0 || (idSeq != null && idSeq < 0)) {
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
        if (idMs < maxMs) {
            return ErrorRespMessage("ERR The ID specified in XADD is equal or smaller than the target stream top item") to null
        }
        val maxSeq = if (idMs == maxMs) {
            existingStream.values.filter { it.id.ms == maxMs }.maxOf { it.id.seq }
        } else {
            -1
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

    private fun generateId(idMs: Long, idSeq: Long?, minimumSeq: Long): StreamEntryId {
        if (idSeq == null) {
            if (idMs == 0L) {
                return StreamEntryId(0, max(minimumSeq, 1))
            }
            return StreamEntryId(idMs, minimumSeq)
        }
        return StreamEntryId(idMs, idSeq)
    }

    private fun longOr(str: String, or: Long?): Long? {
        if (str == "*") {
            return or
        }
        return try {
            str.toLong()
        } catch (e: NumberFormatException) {
            throw IllegalArgumentException("Invalid long: $str")
        }
    }

}
