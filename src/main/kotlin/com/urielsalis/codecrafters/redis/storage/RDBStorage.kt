package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.rdb.parseRDB
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.io.File
import java.io.FileNotFoundException
import java.time.Instant

class RDBStorage(
    private val dir: String = "remote",
    private val dbFileName: String = "remote",
    private var bytes: ByteArray? = null
) : Storage {
    val inMemoryStorage = InMemoryStorage()

    init {
        if (bytes == null) {
            try {
                bytes = File(dir, dbFileName).readBytes()
                val map = parseRDB(bytes!!).toMutableMap()
                map.forEach {
                    inMemoryStorage.set(it.key, it.value.second, it.value.first)
                }
            } catch (e: FileNotFoundException) {
                println("RDB file doesn't exist, continuing with empty DB")
            }
        }
    }

    override fun set(key: String, value: RespMessage, expiry: Instant) =
        inMemoryStorage.set(key, value, expiry)

    override fun get(key: String): RespMessage? = inMemoryStorage.get(key)

    override fun getKeys(pattern: String): ArrayRespMessage = inMemoryStorage.getKeys(pattern)

    override fun getConfig(key: String): ArrayRespMessage? {
        if (key == "dir") {
            return ArrayRespMessage(
                listOf(
                    BulkStringRespMessage("dir"), BulkStringRespMessage(dir)
                )
            )
        }
        if (key == "dbfilename") {
            return ArrayRespMessage(
                listOf(
                    BulkStringRespMessage("dbfilename"), BulkStringRespMessage(dbFileName)
                )
            )
        }
        return null
    }
}
