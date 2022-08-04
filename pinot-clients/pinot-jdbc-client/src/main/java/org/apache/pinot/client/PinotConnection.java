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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.pinot.client.base.AbstractBaseConnection;
import org.apache.pinot.client.controller.PinotControllerTransport;
import org.apache.pinot.client.controller.PinotControllerTransportFactory;
import org.apache.pinot.client.controller.response.ControllerTenantBrokerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotConnection extends AbstractBaseConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
  private org.apache.pinot.client.Connection _session;
  private boolean _closed;
  private String _controllerURL;
  private PinotControllerTransport _controllerTransport;
  public static final String BROKER_LIST = "brokers";

  PinotConnection(String controllerURL, PinotClientTransport transport, String tenant,
      PinotControllerTransport controllerTransport) {
    this(new Properties(), controllerURL, transport, tenant, controllerTransport);
  }

  PinotConnection(Properties properties, String controllerURL, PinotClientTransport transport, String tenant,
      PinotControllerTransport controllerTransport) {
    _closed = false;
    _controllerURL = controllerURL;
    if (controllerTransport == null) {
      _controllerTransport = new PinotControllerTransportFactory().buildTransport();
    } else {
      _controllerTransport = controllerTransport;
    }
    List<String> brokers;
    if (properties.containsKey(BROKER_LIST)) {
      brokers = Arrays.asList(properties.getProperty(BROKER_LIST).split(";"));
    } else {
      brokers = getBrokerList(controllerURL, tenant);
    }
    _session = new org.apache.pinot.client.Connection(properties, brokers, transport);
  }

  public org.apache.pinot.client.Connection getSession() {
    return _session;
  }

  private List<String> getBrokerList(String controllerURL, String tenant) {
    ControllerTenantBrokerResponse controllerTenantBrokerResponse =
        _controllerTransport.getBrokersFromController(controllerURL, tenant);
    return controllerTenantBrokerResponse.getBrokers();
  }

  @Override
  protected void validateState()
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Connection is already closed!");
    }
  }

  @Override
  public void close()
      throws SQLException {
    if (!isClosed()) {
      _session.close();
      _controllerTransport.close();
    }
    _controllerTransport = null;
    _session = null;
    _closed = true;
  }

  @Override
  public Statement createStatement()
      throws SQLException {
    validateState();
    return new PinotStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql)
      throws SQLException {
    validateState();
    return new PinotPreparedStatement(this, sql);
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return _closed;
  }

  @Override
  public DatabaseMetaData getMetaData()
      throws SQLException {
    return new PinotConnectionMetaData(this, _controllerURL, _controllerTransport);
  }
}
