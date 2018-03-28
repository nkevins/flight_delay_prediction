import org.joda.time.{DateTime, Days}

import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup
import net.sf.jweather.metar.Metar
import net.sf.jweather.metar.MetarParser
import net.sf.jweather.metar.WeatherCondition
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.ArrayList
import ca.braunit.weatherparser.metar._
import ca.braunit.weatherparser.common.domain._

object parseWeather extends App {
  val start = DateTime.now.minusDays(6)
  val end   = DateTime.now.plusDays(0)

  val daysCount = Days.daysBetween(start, end).getDays() + 1
  
  val conf = new SparkConf().setAppName("Weather Process").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  // Parse weather data
  (0 until daysCount).map(start.plusDays(_)).foreach(parseWeatherData)
  
  def parseWeatherData(date: DateTime) : Unit = {
    val lines = sc.wholeTextFiles("hdfs://localhost/user/cloudera/weather/raw/" + date.toString("yyyyMMdd") + ".txt").first._2.split("\n")
    
    // Remove comment line started with #
    val filteredComments = lines.filter(x => !x.startsWith("#") && !x.trim().equals("")).map(_.trim())
    
    // Combine data split across multiple lines
    val metarList = filteredComments.mkString(" ").split("=").map(_.trim().split(" ").drop(2).mkString(" "))
    
    // Parse the format
    val parsedMetar = metarList.map(x => {
      try {
        MetarParser.parseReport(x)
      } catch {
        case e: Exception => {
          println("METAR parsing error: " + x)
          null
        }
      }
    })
    
    val metarTuple = parsedMetar.filter(x => x != null).map(x => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
      (sdf.format(x.getDate), x.getWindSpeedInKnots, x.getWindGustsInKnots, x.getTemperatureInCelsius, x.getVisibilityInKilometers, x.getReportString.contains("RA"), x.getReportString.contains("TS"), x.getReportString.contains("SH"))        
    })
    
    val csvData = metarTuple.map(x => x.productIterator.mkString(",")).mkString("\n")
    
    // Save into CSV file
    val conf2 = new Configuration()
    conf2.set("fs.defaultFS", "hdfs://localhost:8020")
    val fs = FileSystem.get(conf2)
    val filePath = new Path("/user/cloudera/weather/staging/" + date.toString("yyyyMMdd") + ".csv")
    if (fs.exists(filePath)) {
      fs.delete(filePath, true)
    }
    
    val os = fs.create(filePath)
    os.write("time,windSpeed,windGust,temp,visibility,RA,TS,SH\n".getBytes)
    os.write(csvData.getBytes)
  }
}