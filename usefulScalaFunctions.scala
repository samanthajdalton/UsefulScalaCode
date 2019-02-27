import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time._
import java.util.Date
import java.text.SimpleDateFormat
val ymd_dateFormatter = new SimpleDateFormat("yyyyMMdd")
val today = new Date()
val todayDate = ymd_dateFormatter.format(today)
val sqlContext =  new SQLContext(sc)
import sqlContext.implicits._

def table(name: String) = sqlContext.table(name)

val dateFormatter = udf(
(date: String) => {
  val fmt = """(\d{4})-(\d{2})-(\d{2}).*""".r
  val fmt(y, m, d) = date.substring(0,10)
  (y + m + d).toInt
})

def addDays(start: String, nDays: Int): String = {
    val startDT = DateTime.parse(start, format.DateTimeFormat.forPattern("yyyyMMdd"))
    val endDT = startDT.plusDays(nDays)
    endDT.toString("yyyyMMdd")
}
def today = DateTime.now().toString("yyyyMMdd")

def daysBetween(start: String, end: String): Int = {
  val startDT = DateTime.parse(start, format.DateTimeFormat.forPattern("yyyyMMdd"))
  val endDT = DateTime.parse(end, format.DateTimeFormat.forPattern("yyyyMMdd"))
  Days.daysBetween(startDT, endDT).getDays()
}

//days between columns

def dayOfWeek(column: Column) = {
    from_unixtime(unix_timestamp(column, "yyyymmdd"), "EEEEE").alias("dow")
}

def dayStamp(column: Column) = unix_timestamp(column, "yyyyMMdd") / 86400

def diffDate(start: Column, end: Column) = {
  val (startTime, endTime) = (unix_timestamp(start), unix_timestamp(end))
  val secondsInADay = 60*60*24
  round((endTime - startTime) / secondsInADay, 2)
}

def addDaysCol(datadate: Column, nDays: Int) = {
  from_unixtime(unix_timestamp(datadate, "yyyyMMdd") + nDays*86400, "yyyyMMdd")
}

def coalesceSave(dataframe: DataFrame, tableName: String, partitions: Int = 1) = {
    dataframe
        .coalesce(partitions)
        .write
        .mode("overwrite")
        .saveAsTable(tableName)
}

def maxCol(left: Column, right: Column) = when(left > right, left).otherwise(left)
def minCol(left: Column, right: Column) = when(left < right, left).otherwise(left)