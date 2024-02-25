package com.urielsalis.codecrafters.redis.storage

import com.urielsalis.codecrafters.redis.resp.ArrayRespMessage
import com.urielsalis.codecrafters.redis.resp.BulkStringRespMessage
import com.urielsalis.codecrafters.redis.resp.RespMessage
import java.time.Instant

class RDBStorage(private val dir: String, private val dbFileName: String) : Storage {
    override fun set(key: String, value: RespMessage, expiry: Instant) {
        TODO()
    }

    override fun get(key: String): RespMessage? {
        TODO()
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
