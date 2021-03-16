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
package org.apache.pinot.integration.tests;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;


public class BasicAuthRealtimeIntegrationTest extends RealtimeClusterIntegrationTest {
  @BeforeClass
  public void setUp()
      throws Exception {
    super.setUp();
    startMinion(null, null);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    stopMinion();
    super.tearDown();
  }

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    return BasicAuthTestUtils.addControllerConfiguration(super.getDefaultControllerConfiguration());
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    return BasicAuthTestUtils.addBrokerConfiguration(super.getDefaultBrokerConfiguration().toMap());
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    return BasicAuthTestUtils.addServerConfiguration(super.getDefaultServerConfiguration().toMap());
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    return BasicAuthTestUtils.addMinionConfiguration(super.getDefaultMinionConfiguration().toMap());
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    return null;
//    // TODO build valid ingestion (and table) config for segment flush
//    return new IngestionConfig(null, new StreamIngestionConfig(Collections
//        .singletonList(Collections.singletonMap(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "10000"))), null,
//        null);
  }

  @Override
  protected void addSchema(Schema schema)
      throws IOException {
    PostMethod response =
        sendMultipartPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schema.toSingleLineJsonString(),
            AUTH_HEADER);
    Assert.assertEquals(response.getStatusCode(), 200);
  }

  @Override
  protected void addTableConfig(TableConfig tableConfig)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString(), AUTH_HEADER);
  }

  @Override
  protected Connection getPinotConnection() {
    if (_pinotConnection == null) {
      _pinotConnection =
          ConnectionFactory.fromZookeeper(ZkStarter.DEFAULT_ZK_STR + "/" + getHelixClusterName(), AUTH_HEADER);
    }
    return _pinotConnection;
  }

  @Override
  protected void testQuery(String pqlQuery, @Nullable List<String> sqlQueries)
      throws Exception {
    ClusterIntegrationTestUtils
        .testPqlQuery(pqlQuery, _brokerBaseApiUrl, getPinotConnection(), sqlQueries, getH2Connection(), AUTH_HEADER);
  }

  @Override
  protected void testSqlQuery(String pinotQuery, @Nullable List<String> sqlQueries)
      throws Exception {
    ClusterIntegrationTestUtils
        .testSqlQuery(pinotQuery, _brokerBaseApiUrl, getPinotConnection(), sqlQueries, getH2Connection(), AUTH_HEADER);
  }

  @Override
  protected void dropRealtimeTable(String tableName)
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType(tableName)),
        AUTH_HEADER);
  }

  // TODO end-to-end test involving segment upload and download sequence with auth
}
