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

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.utils.ReadMode;


public class ExactlyOnceKafkaRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Override
  protected boolean useKafkaTransaction() {
    return true;
  }

  @Override
  protected String getLoadMode() {
    return ReadMode.mmap.name();
  }

  @Override
  public void startController()
      throws Exception {
    Map<String, Object> properties = getDefaultControllerConfiguration();

    properties.put(ControllerConf.ALLOW_HLC_TABLES, false);
    startController(properties);
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    // the first transaction of kafka messages are aborted
    ClusterIntegrationTestUtils
        .pushAvroIntoKafkaWithTransaction(avroFiles, "localhost:" + getKafkaPort(), getKafkaTopic(),
            getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), false);
    // the second transaction of kafka messages are committed
    ClusterIntegrationTestUtils
        .pushAvroIntoKafkaWithTransaction(avroFiles, "localhost:" + getKafkaPort(), getKafkaTopic(),
            getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), true);
  }
}
