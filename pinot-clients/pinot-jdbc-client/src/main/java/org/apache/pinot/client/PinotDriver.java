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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.pinot.client.utils.DriverUtils;
import org.slf4j.LoggerFactory;


public class PinotDriver implements Driver {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotDriver.class);
  private final String SCHEME = "pinot";
  public static final String TENANT = "tenant";

  @Override
  public Connection connect(String url, Properties info)
      throws SQLException {
    try {
      LOGGER.info("Initiating connection to database for url: " + url);
      PinotClientTransport pinotClientTransport = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
      String controllerUrl = DriverUtils.getControllerFromURL(url);
      Preconditions.checkArgument(info.containsKey(TENANT), "Pinot tenant missing in the properties");
      String tenant = info.getProperty(TENANT);
      return new PinotConnection(controllerUrl, pinotClientTransport, tenant);
    } catch (Exception e) {
      throw new SQLException(String.format("Failed to connect to url : %s", url), e);
    }
  }

  @Override
  public boolean acceptsURL(String url)
      throws SQLException {
    String cleanURI = url.substring(5);
    URI uri = URI.create(cleanURI);
    return uri.getScheme().contentEquals(SCHEME);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {
    List<DriverPropertyInfo> propertyInfoList = new ArrayList<>();
    DriverPropertyInfo tenantPropertyInfo = new DriverPropertyInfo("tenant", null);
    tenantPropertyInfo.required = true;
    tenantPropertyInfo.description =
        "Name of the tenant for which to create the JDBC connection. You can only query the tables belonging to the specified tenant";
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
