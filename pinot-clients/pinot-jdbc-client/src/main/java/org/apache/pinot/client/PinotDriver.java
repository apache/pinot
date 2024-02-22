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
package org.apache.pinot.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import io.netty.handler.ssl.SslContext;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.client.controller.PinotControllerTransport;
import org.apache.pinot.client.controller.PinotControllerTransportFactory;
import org.apache.pinot.client.utils.DriverUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.LoggerFactory;


public class PinotDriver implements Driver {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotDriver.class);
  private static final String URI_SCHEME = "pinot";
  public static final String INFO_TENANT = "tenant";
  public static final String DEFAULT_TENANT = "DefaultTenant";
  public static final String INFO_SCHEME = "scheme";
  public static final String INFO_HEADERS = "headers";

  private SslContext _sslContext = null;

  public PinotDriver() { }

  @VisibleForTesting
  public PinotDriver(SslContext sslContext) {
    _sslContext = sslContext;
  }

  /**
   * Created connection to Pinot Controller from provided properties.
   * The following properties can be provided -
   * tenant - Specify the tenant for which this connection is being created. If not provided, DefaultTenant is used.
   * The connection cannot handle queries for tables which are not present in the specified tenant.
   * headers.Authorization - base64 token to query pinot. This is required in case Auth is enabled on pinot cluster.
   * user - Name of the user for which auth is enabled.
   * password - Password associated with the user for which auth is enabled.
   * You can also specify username and password in the URL, e.g. jdbc:pinot://localhost:9000?user=Foo&password=Bar
   * If username and password are specified at multiple places, the precedence takes place in the following order
   * (header.Authorization property) > (username and password in URL) > (user and password specified in properties)
   * @param url  jdbc connection url containing pinot controller machine host:port.
   * example - jdbc:pinot://localhost:9000
   * @param info properties required for creating connection
   * @return JDBC connection object to query pinot
   * @throws SQLException
   */
  @Override
  public Connection connect(String url, Properties info)
      throws SQLException {

      PinotClientTransport pinotClientTransport = null;
      PinotControllerTransport pinotControllerTransport = null;
    try {
      if (!this.acceptsURL(url)) {
        return null;
      }
      LOGGER.info("Initiating connection to database for url: " + url);

      Map<String, String> urlParams = DriverUtils.getURLParams(url);
      info.putAll(urlParams);

      JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
      PinotControllerTransportFactory pinotControllerTransportFactory = new PinotControllerTransportFactory();

      if (info.containsKey(INFO_SCHEME)) {
        factory.setScheme(info.getProperty(INFO_SCHEME));
        pinotControllerTransportFactory.setScheme(info.getProperty(INFO_SCHEME));
        if (info.getProperty(INFO_SCHEME).contentEquals(CommonConstants.HTTPS_PROTOCOL)) {
          if (_sslContext == null) {
            factory.setSslContext(DriverUtils.getSslContextFromJDBCProps(info));
            pinotControllerTransportFactory.setSslContext(factory.getSslContext());
          } else {
            factory.setSslContext(_sslContext);
            pinotControllerTransportFactory.setSslContext(_sslContext);
          }
        }
      }

      Map<String, String> headers = getHeadersFromProperties(info);

      DriverUtils.handleAuth(info, headers);

      if (!headers.isEmpty()) {
        factory.setHeaders(headers);
        pinotControllerTransportFactory.setHeaders(headers);
      }

      pinotClientTransport = factory.withConnectionProperties(info).buildTransport();
      pinotControllerTransport = pinotControllerTransportFactory
              .withConnectionProperties(info)
              .buildTransport();
      String controllerUrl = DriverUtils.getControllerFromURL(url);
      String tenant = info.getProperty(INFO_TENANT, DEFAULT_TENANT);
      return new PinotConnection(info, controllerUrl, pinotClientTransport, tenant, pinotControllerTransport);
    } catch (Exception e) {
      closeResourcesSafely(pinotClientTransport, pinotControllerTransport);
      throw new SQLException(String.format("Failed to connect to url : %s", url), e);
    }
  }

  /**
   * If something goes wrong generating the connection, the transports need to be safely closed to prevent leaks.
   * @param pinotClientTransport connection client transport
   * @param pinotControllerTransport connection controller transport
   */
  private static void closeResourcesSafely(PinotClientTransport pinotClientTransport,
      PinotControllerTransport pinotControllerTransport) {
    try {
      if (pinotClientTransport != null) {
        pinotClientTransport.close();
      }
    } catch (PinotClientException ignored) {
      // ignored
    }

    try {
      if (pinotControllerTransport != null) {
        pinotControllerTransport.close();
      }
    } catch (PinotClientException ignored) {
      // ignored
    }
  }

  private Map<String, String> getHeadersFromProperties(Properties info) {
    return info.entrySet().stream().filter(entry -> entry.getKey().toString().startsWith(INFO_HEADERS + ".")).map(
            entry -> Pair.of(entry.getKey().toString().substring(INFO_HEADERS.length() + 1),
                entry.getValue().toString()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  @Override
  public boolean acceptsURL(String url)
      throws SQLException {
    String cleanURI = url.substring(5);
    URI uri = URI.create(cleanURI);
    return uri.getScheme().contentEquals(URI_SCHEME);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {
    List<DriverPropertyInfo> propertyInfoList = new ArrayList<>();
    DriverPropertyInfo tenantPropertyInfo = new DriverPropertyInfo("tenant", null);
    tenantPropertyInfo.required = true;
    tenantPropertyInfo.description =
        "Name of the tenant for which to create the JDBC connection. You can only query the tables belonging to the "
            + "specified tenant";
    propertyInfoList.add(tenantPropertyInfo);
    return Iterables.toArray(propertyInfoList, DriverPropertyInfo.class);
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger()
      throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  static {
    try {
      PinotDriver driver = new PinotDriver();
      DriverManager.registerDriver(driver);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
