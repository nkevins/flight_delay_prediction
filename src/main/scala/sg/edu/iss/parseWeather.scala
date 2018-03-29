package sg.edu.iss

import org.joda.time.{DateTime, Days}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import ca.braunit.weatherparser.metar._
import ca.braunit.weatherparser.common.domain._

object parseWeather extends App {
  val start = DateTime.now.minusDays(3)
  val end   = DateTime.now.plusDays(0)

  val daysCount = Days.daysBetween(start, end).getDays() + 1
  
  val conf = new SparkConf().setAppName("Weather Process").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  // Parse weather data
  (0 until daysCount).map(start.plusDays(_)).foreach(parseWeatherData)
  
  def findWeatherDescriptor(weathers: List[Weather], descriptor: String): Boolean = {
    weathers.exists(x => x.getDescriptorCode == descriptor)
  }
  
  def findWeatherPrecipitation(weathers: List[Weather], descriptor: String): Boolean = {
    weathers.exists(x => x.getPrecipitationCode == descriptor)
  }
  
  def findWeatherObscuration(weathers: List[Weather], descriptor: String): Boolean = {
    weathers.exists(x => x.getObscurationCode == descriptor)
  }
  
  def findWeatherIntensity(weathers: List[Weather]): Int = {
    if (weathers.exists(x => x.getIntensityCode == "+")) return 2
    if (weathers.exists(x => x.getIntensityCode == "-")) return 1
    return 0
  }
  
  def parseWeatherData(date: DateTime) : Unit = {
    val lines = sc.wholeTextFiles("hdfs://localhost/user/cloudera/weather/raw/" + date.toString("yyyyMMdd") + ".txt").first._2.split("\n")
    
    // Remove comment line started with #
    val filteredComments = lines.filter(x => !x.startsWith("#") && !x.trim().equals("")).map(_.trim())
    
    // Combine data split across multiple lines
    val metarList = filteredComments.mkString(" ").split("=").map(_.trim().split(" ").drop(2).mkString(" "))
    
    // Parse the format
    val parsedMetar = metarList.map(x => {
      MetarDecoder.decodeMetar(x).getMetar
    })
    
    import scala.collection.JavaConversions._
    
    val metarTuple = parsedMetar.map(x => {
      try {
        val reportTime = date.toString("yyyy-MM-") + x.getReportTime.getDayOfMonth + " " + String.format("%02d", x.getReportTime.getHour) + ":" + String.format("%02d", x.getReportTime.getMinute) + ":00";
        
        val pw = x.getPresentWeather.toList
        (reportTime, x.getWind.getWindSpeed, x.getWind.getWindSpeedGusts, x.getTemperatureAndDewPoint.getTemperature, x.getVisibility.get(0).getVisibility, findWeatherIntensity(pw), findWeatherPrecipitation(pw, "RA"), findWeatherDescriptor(pw, "TS"), findWeatherDescriptor(pw, "SH"), findWeatherObscuration(pw, "BR"), findWeatherObscuration(pw, "FG"), findWeatherObscuration(pw, "HZ"))
      } catch {
        case e:Exception => ("","","","","","","","","","","","")
      }
    })
    

    val csvData = metarTuple.map(x => x.productIterator.mkString(",")).mkString("\n").replace("null", "")
    
    // Save into CSV file
    val conf2 = new Configuration()
    conf2.set("fs.defaultFS", "hdfs://localhost:8020")
    val fs = FileSystem.get(conf2)
    val filePath = new Path("/user/cloudera/weather/staging/" + date.toString("yyyyMMdd") + ".csv")
    if (fs.exists(filePath)) {
      fs.delete(filePath, true)
    }
    
    val os = fs.create(filePath)
    os.write("time,windSpeed,windGust,temp,visibility,intensity,RA,TS,SH,BR,FG,HZ\n".getBytes)
    os.write(csvData.getBytes)
  }
}