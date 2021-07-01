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
package org.apache.pinot.client.utils;

import java.net.URI;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DriverUtils {
  public static final String SCHEME = "jdbc";
  public static final String DRIVER = "pinot";
  public static final Logger LOG = LoggerFactory.getLogger(DriverUtils.class);
  public static final String QUERY_SEPERATOR = "&";
  public static final String PARAM_SEPERATOR = "=";
  public static final String CONTROLLER = "controller";

  private DriverUtils() {
  }

  public static List<String> getBrokersFromURL(String url) {
    if (url.toLowerCase().startsWith("jdbc:")) {
      url = url.substring(5);
    }
    URI uri = URI.create(url);
    return getBrokersFromURI(uri);
  }

  public static List<String> getBrokersFromURI(URI uri) {
    String brokerUrl = String.format("%s:%d", uri.getHost(), uri.getPort());
    List<String> brokerList = Collections.singletonList(brokerUrl);
    return brokerList;
  }

  public static String getURIFromBrokers(List<String> brokers) {
    try {
      String broker = brokers.get(0);
      String[] hostPort = broker.split(":");
      URI uri = new URI(SCHEME + ":" + DRIVER, hostPort[0], hostPort[1]);
      return uri.toString();
    } catch (Exception e) {
      LOG.warn("Broker list is either empty or has incorrect format", e);
    }
    return "";
  }

  public static String getControllerFromURL(String url) {
    if (url.toLowerCase().startsWith("jdbc:")) {
      url = url.substring(5);
    }
    URI uri = URI.create(url);
    String controllerUrl = String.format("%s:%d", uri.getHost(), uri.getPort());
    return controllerUrl;
  }

  public static Integer getSQLDataType(String columnDataType) {
    if (columnDataType == null) {
      return Types.VARCHAR;
    }
    Integer columnsSQLDataType;
    switch (columnDataType) {
      case "STRING":
        columnsSQLDataType = Types.VARCHAR;
        break;
      case "INT":
        columnsSQLDataType = Types.INTEGER;
        break;
      case "LONG":
        columnsSQLDataType = Types.INTEGER;
        break;
      case "FLOAT":
        columnsSQLDataType = Types.FLOAT;
        break;
      case "DOUBLE":
        columnsSQLDataType = Types.DOUBLE;
        break;
      case "BOOLEAN":
        columnsSQLDataType = Types.BOOLEAN;
        break;
      case "BYTES":
        columnsSQLDataType = Types.BINARY;
        break;
      case "TIMESTAMP":
        columnsSQLDataType = Types.TIMESTAMP;
        break;
      default:
        columnsSQLDataType = Types.NULL;
    }
    return columnsSQLDataType;
  }

  public static String getJavaClassName(String columnDataType) {
    if (columnDataType == null) {
      return String.class.getTypeName();
    }
    String columnsJavaClassName;
    switch (columnDataType) {
      case "STRING":
        columnsJavaClassName = String.class.getTypeName();
        break;
      case "INT":
        columnsJavaClassName = Integer.class.getTypeName();
        break;
      case "LONG":
        columnsJavaClassName = Long.class.getTypeName();
        break;
      case "FLOAT":
        columnsJavaClassName = Float.class.getTypeName();
        break;
      case "DOUBLE":
        columnsJavaClassName = Double.class.getTypeName();
        break;
      case "BOOLEAN":
        columnsJavaClassName = Boolean.class.getTypeName();
        break;
      case "BYTES":
        columnsJavaClassName = byte.class.getTypeName();
        break;
      case "TIMESTAMP":
        columnsJavaClassName = Timestamp.class.getTypeName();
        break;
      default:
        columnsJavaClassName = String.class.getTypeName();
    }
    return columnsJavaClassName;
  }
}
