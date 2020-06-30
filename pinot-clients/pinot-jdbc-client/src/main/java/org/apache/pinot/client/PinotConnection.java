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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import org.apache.pinot.client.base.AbstractBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotConnection extends AbstractBaseConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
  private static org.apache.pinot.client.Connection _session;

  PinotConnection(List<String> brokerList, PinotClientTransport transport) {
    _session = new org.apache.pinot.client.Connection(brokerList, transport);
  }

  public org.apache.pinot.client.Connection getSession() {
    return _session;
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
    }
    _session = null;
  }

  @Override
  public Statement createStatement()
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Connection is already closed!");
    }
    return new PinotStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql)
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Connection is already closed!");
    }
    return new PinotPreparedStatement(this, sql);
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return (_session == null);
  }

  @Override
  public DatabaseMetaData getMetaData()
      throws SQLException {
    return new PinotConnectionMetaData(this);
  }
}
