package com.szq80140.datasync

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*
import java.util.regex.Pattern

/**
 * @author sunzq
 * @since 2017/9/29
 */

enum class SqlFieldType {
    /**
     * int
     */
    I,
    /**
     * date
     */
    D,
    /**
     * float
     */
    F,
    /**
     * Long
     */
    L,
    /**
     * string
     */
    S
}

data class SqlField(var type: SqlFieldType?, var field: String?)

class SqlInitializer(sql: String) {

    private val logger = LoggerFactory.getLogger(SqlInitializer::class.java)

    private val paramPattern = "#(?<type>[DFILS])\\{\\s*(?<field>[\\w$]+)\\s*}"

    private val fieldPattern = Pattern.compile(paramPattern)

    private val sqlFields = ArrayList<SqlField>()

    private val sqlTemplate: String = sql.replace(paramPattern.toRegex(), " ? ")

    init {
        val matcher = fieldPattern.matcher(sql)
        while (matcher.find()) {
            sqlFields.add(SqlField(SqlFieldType.valueOf(matcher.group("type")), matcher.group("field")))
        }
    }

    fun execute(params: Map<String, String>, connection: Connection): Int {
        val startTs = System.currentTimeMillis()
        val simpleDateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        connection.prepareStatement(this.sqlTemplate).use { preparedStatement ->
            for (i in this.sqlFields.indices) {
                val sqlField = this.sqlFields[i]
                val fieldValue = params[sqlField.field] ?: ""
                val paramIndex = i + 1
                logger.debug("set " + sqlField.field + "=" + fieldValue + "@" + paramIndex)
                when (sqlField.type) {
                    SqlFieldType.D -> preparedStatement.setTimestamp(paramIndex,
                            getOrDefault({ Timestamp(simpleDateFormat.parse(fieldValue).time) }, Timestamp(0)))
                    SqlFieldType.F -> preparedStatement.setDouble(paramIndex, getOrDefault({ fieldValue.toDouble() }, 0.0))
                    SqlFieldType.I -> preparedStatement.setInt(paramIndex, getOrDefault({ fieldValue.toInt() }, 0))
                    SqlFieldType.L -> preparedStatement.setLong(paramIndex, getOrDefault({ fieldValue.toLong() }, 0L))
                    else -> preparedStatement.setString(paramIndex, fieldValue)
                }
            }
            val executeUpdate = preparedStatement.executeUpdate()
            logger.info(this.sqlTemplate + "|" + params + "|rowEffected:" + executeUpdate + "|tu:" + (System.currentTimeMillis() - startTs))
            return executeUpdate
        }
    }

    private fun <T> getOrDefault(block: () -> T, defaultV: T): T {
        return try {
            block()
        } catch (e: Exception) {
            defaultV
        }
    }

}
