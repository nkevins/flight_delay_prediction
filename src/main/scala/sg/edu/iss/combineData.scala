package sg.edu.iss

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.joda.time.{DateTime, Days, DateTimeZone, Hours, Minutes}
import org.joda.time.format.DateTimeFormat
import scala.reflect.api.materializeTypeTag

object combineData extends App {
  
  val start = DateTime.now.minusDays(3)
  val end   = DateTime.now.plusDays(0)

  val daysCount = Days.daysBetween(start, end).getDays() + 1
  val spark = SparkSession.builder().master("local").appName("Data Processor").getOrCreate()
  import spark.implicits._
  
  
  
  // Combine schedule + weather data
  (0 until daysCount).map(start.plusDays(_)).foreach(combineData)  
  
  
  
  def combineData(date: DateTime) : Unit = {
    val data = spark.read.json("hdfs://localhost/user/cloudera/cag/" + date.toString("yyyyMMdd") + ".json")
    
    // Get flight schedule data
    val flightDf = data.select(explode($"carriers").as("flights"))  
    val flightDataDf = flightDf
      .select($"flights.flightNo", $"flights.airlineCode", $"flights.airportCode", $"flights.from", $"flights.scheduledDatetime", $"flights.estimatedTime", $"flights.status", $"flights.statusCode")
      .filter($"flights.statusCode" === "AR")
      .withColumn("metarTime", appendMetarTime($"scheduledDateTime", $"estimatedTime")).as("flights")
      
    // Get weather data
    val weatherDfNow = spark.read.format("csv").option("header", "true").load("hdfs://localhost/user/cloudera/weather/staging/" + date.toString("yyyyMMdd") + ".csv")
    val weatherDfYesterday = spark.read.format("csv").option("header", "true").load("hdfs://localhost/user/cloudera/weather/staging/" + date.minusDays(1).toString("yyyyMMdd") + ".csv")
    
    val allWeather = weatherDfNow.union(weatherDfYesterday).as("weather")
    
    val finalDf = flightDataDf
      .withColumn("delayed", getDelayStatus($"scheduledDateTime", $"estimatedTime"))
      .join(allWeather, $"flights.metarTime" === $"weather.time", "left_outer")
  
    val csvContent = finalDf.coalesce(1).write.format("csv").mode(SaveMode.Overwrite)
                      .option("header", "true").save("hdfs://localhost/user/cloudera/flightweather/" + date.toString("yyyyMMdd"))
  }
  
    
  def appendMetarTime = udf { (scheduledDateTime: String, estimatedTime: String) => {    
      val formatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss")
      val formatter2 = DateTimeFormat.forPattern("yyyyMMdd")
      val formatter3 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      val scheduledArrivalTime = formatter.withZone(DateTimeZone.forID("Asia/Singapore")).parseDateTime(scheduledDateTime)
      
      try {
        val etaWithoutDay = formatter.withZone(DateTimeZone.forID("Asia/Singapore")).parseDateTime(scheduledDateTime.substring(0, 8) + " " + estimatedTime + ":00")
        
        val hrsDiff = Math.abs(Hours.hoursBetween(scheduledArrivalTime, etaWithoutDay).getHours)
        val actualEta = if (hrsDiff > 20) {
          if (scheduledArrivalTime.getHourOfDay >= 23) {
            etaWithoutDay.plusDays(1)
          } else {
            etaWithoutDay.minusDays(1)  
          }
        } else {
          etaWithoutDay
        }
        
        val newActualEta = if (actualEta.getMinuteOfHour > 30) {
          actualEta.minusMinutes(actualEta.getMinuteOfHour - 30)
        } else {
          actualEta.minusMinutes(actualEta.getMinuteOfHour)
        }
        
        // Convert from SGT to UTC
        newActualEta.minusHours(8).toString("yyyy-MM-dd HH:mm:ss")
      } catch {
        case e: Exception => { "1990-01-01 00:00:00" }
      }
    }    
  }
  
  def getDelayStatus = udf { (scheduledDateTime: String, estimatedTime: String) => {
      val formatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss")
      val formatter2 = DateTimeFormat.forPattern("yyyyMMdd")
      val formatter3 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      val scheduledArrivalTime = formatter.withZone(DateTimeZone.forID("Asia/Singapore")).parseDateTime(scheduledDateTime)
      
      try {
        val etaWithoutDay = formatter.withZone(DateTimeZone.forID("Asia/Singapore")).parseDateTime(scheduledDateTime.substring(0, 8) + " " + estimatedTime + ":00")
      
        val hrsDiff = Math.abs(Hours.hoursBetween(scheduledArrivalTime, etaWithoutDay).getHours)
        val actualEta = if (hrsDiff > 20) {
          if (scheduledArrivalTime.getHourOfDay >= 23) {
            etaWithoutDay.plusDays(1)
          } else {
            etaWithoutDay.minusDays(1)  
          }
        } else {
          etaWithoutDay
        }  
        
        val actualDiff =  Minutes.minutesBetween(scheduledArrivalTime, actualEta).getMinutes
        if (actualDiff > 15) {
          true
        } else {
          false
        }
      } catch {
        case e: Exception => { false }
      }
    }
  }
}