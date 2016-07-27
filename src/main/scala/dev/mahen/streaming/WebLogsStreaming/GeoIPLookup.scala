package dev.mahen.streaming.WebLogsStreaming

import com.maxmind.geoip.LookupService
import java.io.File
import org.apache.commons.validator.routines.InetAddressValidator

class GeoIPLookup(val ip: String) {

  def apply(): (String, String, Int, String, String) = {

    val GeoIpFile = this.getClass.getClassLoader.getResource("GeoLiteCity.dat");

    val geoIpF = Thread.currentThread().getContextClassLoader().getResource("GeoLiteCity.dat")

    //    println("GeoIpFile.getPath : " + IOUtils.toString(GeoIpFile))

    //    val cl = new LookupService(GeoIpFile.getPath, LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE)

    //    val cl = new LookupService(GeoIpFile.getFile, LookupService.GEOIP_MEMORY_CACHE)

    val cl = new LookupService(new File("/tmp/GeoLiteCity.dat"), LookupService.GEOIP_MEMORY_CACHE)

    println("IP Input : " + ip)
    
    val notIn = List("102.242.18.229")
    if (InetAddressValidator.getInstance.isValid(ip) && !notIn.contains(ip)) {

    
      val location = cl.getLocation(ip)
      cl.close()
      (location.countryName, location.city, location.area_code, location.countryCode, location.region)

    } else {
      (null, null, 0, null, null)

    }

  }

}

object GeoIPLookup {

  def apply(fileName: String) {

    GeoIPLookup(fileName)
  }
}