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

import io.netty.handler.ssl.SslContext;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.pinot.common.auth.BasicAuthUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DriverUtils {
  public static final String SCHEME = "jdbc:";
  public static final String DRIVER = "pinot";
  public static final Logger LOG = LoggerFactory.getLogger(DriverUtils.class);
  private static final String LIMIT_STATEMENT_REGEX = "\\s(limit)\\s";

  // SSL Properties
  public static final String PINOT_JDBC_TLS_PREFIX = "pinot.jdbc.tls";

  // Auth Properties
  public static final String USER_PROPERTY = "user";
  public static final String PASSWORD_PROPERTY = "password";
  public static final String AUTH_HEADER = "Authorization";

  private DriverUtils() {
  }

  public static SslContext getSslContextFromJDBCProps(Properties properties) {
    TlsConfig tlsConfig = TlsUtils.extractTlsConfig(
        new PinotConfiguration(new MapConfiguration(properties)), PINOT_JDBC_TLS_PREFIX);
    TlsUtils.installDefaultSSLSocketFactory(tlsConfig);
    return TlsUtils.buildClientContext(tlsConfig);
  }

  public static void handleAuth(Properties info, Map<String, String> headers)
      throws SQLException {

    if (info.containsKey(USER_PROPERTY) && !headers.containsKey(AUTH_HEADER)) {
      String username = info.getProperty(USER_PROPERTY);
      String password = info.getProperty(PASSWORD_PROPERTY, "");
      if (StringUtils.isAnyEmpty(username, password)) {
        throw new SQLException("Empty username or password provided.");
      }
      String authToken = BasicAuthUtils.toBasicAuthToken(username, password);
      headers.put(AUTH_HEADER, authToken);
    }
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
      URI uri = new URI(SCHEME + DRIVER, hostPort[0], hostPort[1]);
      return uri.toString();
    } catch (Exception e) {
      LOG.warn("Broker list is either empty or has incorrect format", e);
    }
    return "";
  }

  public static String getControllerFromURL(String url) {
    if (url.regionMatches(true, 0, SCHEME, 0, SCHEME.length())) {
      url = url.substring(5);
    }
    URI uri = URI.create(url);
    String controllerUrl = String.format("%s:%d", uri.getHost(), uri.getPort());
    return controllerUrl;
  }

  public static Map<String, String> getURLParams(String url) {
    if (url.regionMatches(true, 0, SCHEME, 0, SCHEME.length())) {
      url = url.substring(SCHEME.length());
    }
    URI uri = URI.create(url);
    List<NameValuePair> params = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);

    Map<String, String> paramsMap = new HashMap<>();
    for (NameValuePair param : params) {
      paramsMap.put(param.getName(), param.getValue());
    }

    return paramsMap;
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
      case "BIG_DECIMAL":
        columnsSQLDataType = Types.DECIMAL;
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
        break;
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
      case "BIG_DECIMAL":
        columnsJavaClassName = BigDecimal.class.getTypeName();
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
        break;
    }
    return columnsJavaClassName;
  }

  public static boolean queryContainsLimitStatement(String query) {
    Pattern pattern = Pattern.compile(LIMIT_STATEMENT_REGEX, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(query);
    return matcher.find();
  }

  public static String enableQueryOptions(String sql, Map<String, Object> options) {
    StringBuilder optionsBuilder = new StringBuilder();
    for (Map.Entry<String, Object> optionEntry : options.entrySet()) {
      if (!sql.contains(optionEntry.getKey())) {
        optionsBuilder.append(DriverUtils.createSetQueryOptionString(optionEntry.getKey(), optionEntry.getValue()));
      }
    }
    optionsBuilder.append(sql);
    return optionsBuilder.toString();
  }

  public static String createSetQueryOptionString(String optionKey, Object optionValue) {
    StringBuilder optionBuilder = new StringBuilder();
    optionBuilder.append("SET ").append(optionKey);

    if (optionValue != null) {
      optionBuilder.append('=');

      if (optionValue instanceof Boolean) {
        optionBuilder.append(((Boolean) optionValue).booleanValue());
      } else if (optionValue instanceof Integer || optionValue instanceof Long) {
        optionBuilder.append(((Number) optionValue).longValue());
      } else if (optionValue instanceof Float || optionValue instanceof Double) {
        optionBuilder.append(((Number) optionValue).doubleValue());
      } else {
        throw new IllegalArgumentException(
            "Option Type " + optionValue.getClass().getSimpleName() + " is not supported.");
      }
    }

    optionBuilder.append(";\n");
    return optionBuilder.toString();
  }
}
