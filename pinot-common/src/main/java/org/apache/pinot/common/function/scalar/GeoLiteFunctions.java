/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function.scalar;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import java.io.InputStream;
import java.net.InetAddress;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GeoLiteFunctions {

  private static DatabaseReader _asnReader = null;
  private static DatabaseReader _cityReader = null;
  private final static Logger logger = LoggerFactory.getLogger(GeoLiteFunctions.class);

  private GeoLiteFunctions() {
  }

  static {
    try {
      InputStream asnIs = GeoLiteFunctions.class.getClassLoader().getResourceAsStream("GeoLite2-ASN.mmdb");
      InputStream cityIs = GeoLiteFunctions.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
      _asnReader = new DatabaseReader.Builder(asnIs).build();
      _cityReader = new DatabaseReader.Builder(cityIs).build();
    } catch (Exception | Error e) {
      logger.error("load GeoLite lib files error: ", e);
    }
  }

  @ScalarFunction
  public static String getAsnJsonFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      AsnResponse response = _asnReader.asn(ipAddress);
      return response.toJson();
    } catch (Exception | Error e) {
      logger.error("GeoLite getAsnJson error for IP [{}]", ipAddr, e);
    }
    return null;
  }

  @ScalarFunction
  public static String getAsnNumberFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      AsnResponse response = _asnReader.asn(ipAddress);
      return String.valueOf(response.getAutonomousSystemNumber());
    } catch (Exception | Error e) {
      logger.error("GeoLite getAsnNumber error: ", e);
    }
    return null;
  }

  @ScalarFunction
  public static String getAsnNameFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      AsnResponse response = _asnReader.asn(ipAddress);
      return response.getAutonomousSystemOrganization();
    } catch (Exception | Error e) {
      logger.error("GeoLite getAsnName error: ", e);
    }
    return null;
  }

  @ScalarFunction
  public static String getContinentFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      CityResponse response = _cityReader.city(ipAddress);
      return response.getContinent().getName();
    } catch (Exception | Error e) {
      logger.error("GeoLite getContinent error: ", e);
    }
    return null;
  }

  @ScalarFunction
  public static String getCountryFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      CityResponse response = _cityReader.city(ipAddress);
      return response.getCountry().getName();
    } catch (Exception | Error e) {
      logger.error("GeoLite getCountry error: ", e);
    }
    return null;
  }

  @ScalarFunction
  public static String getCountryCodeFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      CityResponse response = _cityReader.city(ipAddress);
      return response.getCountry().getIsoCode();
    } catch (Exception | Error e) {
      logger.error("GeoLite getCountryCode error: ", e);
    }
    return null;
  }

  @ScalarFunction
  public static String getTimeZoneFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      CityResponse response = _cityReader.city(ipAddress);
      return response.getLocation().getTimeZone();
    } catch (Exception | Error e) {
      logger.error("GeoLite getTimeZone error: ", e);
    }
    return null;
  }

  @ScalarFunction
  public static Double getLatitudeFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      CityResponse response = _cityReader.city(ipAddress);
      return response.getLocation().getLatitude();
    } catch (Exception | Error e) {
      logger.error("GeoLite getLatitude error: ", e);
    }
    return null;
  }

  @ScalarFunction
  public static Double getLongitudeFromIP(String ipAddr) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ipAddr);
      CityResponse response = _cityReader.city(ipAddress);
      return response.getLocation().getLongitude();
    } catch (Exception | Error e) {
      logger.error("GeoLite getLongitude error: ", e);
    }
    return null;
  }
}
