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

import java.sql.Statement;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotConnectionTest {
  private DummyPinotClientTransport
      _dummyPinotClientTransport = new DummyPinotClientTransport();
  private PinotClientTransportFactory _previousTransportFactory = null;

  @Test
  public void createStatementTest() throws Exception {
    PinotConnection pinotConnection  = new PinotConnection(Collections.singletonList("dummy"), _dummyPinotClientTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
  }

  @BeforeClass
  public void overridePinotClientTransport() {
    _previousTransportFactory = ConnectionFactory._transportFactory;
    ConnectionFactory._transportFactory = new DummyPinotClientTransportFactory(_dummyPinotClientTransport);
  }

  @AfterClass
  public void resetPinotClientTransport() {
    ConnectionFactory._transportFactory = _previousTransportFactory;
  }
}
