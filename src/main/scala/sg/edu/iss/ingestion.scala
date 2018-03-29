package sg.edu.iss

import org.joda.time.{DateTime, Days}
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.jsoup.Jsoup

object ingestion extends App {
  val start = DateTime.now.minusDays(3)
  val end   = DateTime.now.plusDays(0)

  val daysCount = Days.daysBetween(start, end).getDays() + 1
  
  // Get flight schedule from CAG
  (0 until daysCount + 1).map(start.plusDays(_)).foreach(getFlightShedule)
  
  // Get raw weather data
  (0 until daysCount).map(start.plusDays(_)).foreach(getWeather)  
  
  
  
  
  def getFlightShedule(date: DateTime) : Unit = {    
    val rawdata = Source.fromURL("http://www.changiairport.com/cag-web/flights/arrivals?date=" + date.toString("yyyyMMdd") + "&lang=en_US&callback=JSON_CALLBACK").mkString
    
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:8020")
    val fs = FileSystem.get(conf)
    val filePath = new Path("/user/cloudera/cag/" + date.toString("yyyyMMdd") + ".json")
    if (fs.exists(filePath)) {
      fs.delete(filePath, true)
    }
    
    val os = fs.create(filePath)
    os.write(rawdata.getBytes)
    
    Thread.sleep(1000)
  }
  
  def getWeather(date: DateTime) : Unit = {
    val html = Jsoup.connect("https://www.ogimet.com/display_metars2.php?lang=en&lugar=WSSS&tipo=SA&ord=DIR&nil=NO&fmt=txt&ano=" + date.toString("yyyy") + "&mes=" + date.toString("MM") + "&day=" + date.toString("dd") + "&hora=00&anof=" + date.toString("yyyy") + "&mesf=" + date.toString("MM") + "&dayf=" + date.toString("dd") + "&horaf=23&minf=59&send=send").get()
    val rawdata = html.getElementsByTag("pre").first().text()
    
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://localhost:8020")
    val fs = FileSystem.get(conf)
    val filePath = new Path("/user/cloudera/weather/raw/" + date.toString("yyyyMMdd") + ".txt")
    if (fs.exists(filePath)) {
      fs.delete(filePath, true)
    }
    
    val os = fs.create(filePath)
    os.write(rawdata.getBytes)
    
    Thread.sleep(1000)
  }
}