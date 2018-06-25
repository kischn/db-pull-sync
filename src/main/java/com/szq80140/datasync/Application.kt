package com.szq80140.datasync

import com.alibaba.fastjson.JSON
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.io.FileInputStream
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

/**
 *
 *
 * @author sunzq
 */
class Application

data class CdcConfig(val tableName: String,
                     val querySQL: String,
                     val updateSQL: String,
                     val updateInitializer: SqlInitializer,
                     val insertSQL: String,
                     val insertInitializer: SqlInitializer,
                     val lastUpdate: Long)

fun main(args: Array<String>) {
    // load properties
    val properties = Properties()
    properties.load(Application::class.java.classLoader.getResourceAsStream("application.properties"))
    val file = File("application.properties")
    if (file.exists()) {
        properties.load(FileInputStream(file))
    }

    // create connection pool
    val dataSourceTarget = createDataSource(properties, "cdc.target")
    val dataSourceSrc = createDataSource(properties, "cdc.src")
    val cdcConfigDataSource =
            if ("target" == properties.getProperty("cdc.config.datasource")) dataSourceTarget else dataSourceSrc

    // kafka sender
    val kafkaConfig = HashMap<String, Any>()
    kafkaConfig[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    kafkaConfig[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    kafkaConfig[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = properties.getProperty("kafka.bootstrap-servers")
    kafkaConfig[ProducerConfig.RETRIES_CONFIG] = 7
    val kafkaTopic = properties.getProperty("kafka.topic")
    val kafkaProducer = KafkaProducer<String, String>(kafkaConfig)

    // create schedule
    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({
        val simpleDateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        loadCdcConfig(cdcConfigDataSource).forEach { config ->
            println("update table: ${config.tableName}")
            val adder = LongAdder()
            try {
                dataSourceSrc.connection.use { conn ->
                    conn.prepareStatement(config.querySQL).use { preparedStatement ->
                        preparedStatement.setTimestamp(1, Timestamp(config.lastUpdate))
                        preparedStatement.executeQuery().use { rs ->
                            while (rs.next()) {
                                adder.increment()
                                val dataMap = getResultSetMap(rs)
                                dataSourceTarget.connection.use { connectionIn ->
                                    dataMap["TS9254"]?.let { dateStr ->
                                        // update or insert
                                        println("start insert/update")
                                        val startTs = System.currentTimeMillis();
                                        val updateRows = config.updateInitializer.execute(dataMap, connectionIn)
                                        if (updateRows == 0) {
                                            println("data not exists, do insert")
                                            config.insertInitializer.execute(dataMap, connectionIn)
                                        }
                                        println("update or insert time use:" + (System.currentTimeMillis() - startTs))
                                        // update CDC_CONFIG_SQL lastUpdateTime column
                                        val ts = simpleDateFormat.parse(dateStr).time
                                        updateLastUpdateTime(config, ts, connectionIn)
                                        // send to kafka
                                        kafkaProducer.send(ProducerRecord(kafkaTopic, JSON.toJSONString(dataMap)))
                                        println("finished sync tu:" + (System.currentTimeMillis() - startTs))
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                e.printStackTrace()
            } finally {
                println("${config.tableName} dataSize: ${adder.toLong()}")
            }
        }
    }, 1, 1, TimeUnit.MINUTES)

    // block main thread
    val scanner = Scanner(System.`in`)
    while (scanner.hasNext()) {
        if (scanner.nextLine() == "quit") {
            println("bye")
            System.exit(0)
        } else {
            println("input `quit` or `Ctrl+D` to quit this program.")
        }
    }
}

private fun createDataSource(properties: Properties, prefix: String): BasicDataSource {
    val dataSourceIn = BasicDataSource()
    dataSourceIn.driverClassName = properties.getProperty("$prefix.jdbc.driverClassName")
    dataSourceIn.url = properties.getProperty("$prefix.jdbc.url")
    dataSourceIn.username = properties.getProperty("$prefix.jdbc.username")
    dataSourceIn.password = properties.getProperty("$prefix.jdbc.password")
    dataSourceIn.maxTotal = 3
    return dataSourceIn
}

fun updateLastUpdateTime(config: CdcConfig, ts: Long, conn: Connection) {
    val updateSQL = "UPDATE CDC_CONFIG_SQL SET LAST_UPDATE = ? WHERE CDC_TABLE = ? AND (LAST_UPDATE IS NULL OR LAST_UPDATE < ?)"
    conn.prepareStatement(updateSQL).use { preparedStatement ->
        preparedStatement.setTimestamp(1, Timestamp(ts))
        preparedStatement.setString(2, config.tableName)
        preparedStatement.setTimestamp(3, Timestamp(ts))
        preparedStatement.executeUpdate()
    }
}

private fun getResultSetMap(resultSet: ResultSet): Map<String, String> {
    val resultMap = java.util.HashMap<String, String>(20)
    val metaData = resultSet.metaData
    val columnCount = metaData.columnCount
    val simpleDateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (i in 1..columnCount) {
        val columnLabel = metaData.getColumnLabel(i).toUpperCase()
        when (metaData.getColumnType(i)) {
            Types.DATE, Types.TIMESTAMP -> {
                resultSet.getObject(i)?.let { obj ->
                    resultMap[columnLabel] = simpleDateFormat.format(obj)
                }
            }
            else -> resultMap[columnLabel] = resultSet.getString(i) ?: ""
        }
    }
    return resultMap
}

fun loadCdcConfig(basicDataSource: BasicDataSource): List<CdcConfig> {
    val configList = ArrayList<CdcConfig>()
    try {
        basicDataSource.connection.use { conn ->
            val sql = "SELECT CDC_TABLE, INSERT_SQL, UPDATE_SQL, QUERY_SQL, LAST_UPDATE FROM CDC_CONFIG_SQL"
            conn.prepareStatement(sql).use { preparedStatement ->
                preparedStatement.executeQuery().use { rs ->
                    while (rs.next()) {
                        configList.add(CdcConfig(
                                rs.getString("CDC_TABLE"),
                                rs.getString("QUERY_SQL"),
                                rs.getString("UPDATE_SQL"),
                                SqlInitializer(rs.getString("UPDATE_SQL")),
                                rs.getString("INSERT_SQL"),
                                SqlInitializer(rs.getString("INSERT_SQL")),
                                (rs.getTimestamp("LAST_UPDATE")?.time ?: 0)
                        ))
                    }
                }
            }
        }
    } catch (exception: Exception) {
        exception.printStackTrace()
    }
    println("configListSize:$configList")
    return configList
}