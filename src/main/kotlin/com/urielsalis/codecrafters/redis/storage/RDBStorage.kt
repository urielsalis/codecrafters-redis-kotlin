package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.rdb.parseRDB
import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import java.io.File
import java.io.FileNotFoundException

class RDBStorage(
    private val dir: String = "remote",
    private val dbFileName: String = "remote",
    private var bytes: ByteArray? = null
) : InMemoryStorage() {
    init {
        if (bytes == null) {
            try {
                bytes = File(dir, dbFileName).readBytes()
                val map = parseRDB(bytes!!).toMutableMap()
                map.forEach {
                    set(it.key, it.value.second, it.value.first)
                }
            } catch (e: FileNotFoundException) {
                println("RDB file doesn't exist, continuing with empty DB")
            }
        }
    }

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
