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
package org.apache.pinot.connector.flink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminTransport;
import org.apache.pinot.connector.flink.common.FlinkRowGenericRowConverter;
import org.apache.pinot.connector.flink.sink.PinotSink;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;


/**
 * A quick start to populate a segment into Pinot Table using the connector. Please run the GenericQuickStart to create
 * the offline table of all Starbucks store locations in US, and then run this quick start to populate other Starbucks
 * stores in the rest of the world.
 */
public final class FlinkQuickStart {

  public static final RowTypeInfo TEST_TYPE_INFO =
      new RowTypeInfo(new TypeInformation[]{Types.FLOAT, Types.FLOAT, Types.STRING, Types.STRING},
          new String[]{"lon", "lat", "address", "name"});
  private static final String DEFAULT_CONTROLLER_URL = "http://localhost:9000";

  private static List<Row> loadData()
      throws IOException {
    List<Row> rows = new ArrayList<>();
    ClassLoader classLoader = FlinkQuickStart.class.getClassLoader();
    final URL resource = classLoader.getResource("starbucks-stores-world.csv");
    try (BufferedReader br = new BufferedReader(new InputStreamReader(resource.openStream()))) {
      String line;
      while ((line = br.readLine()) != null) {
        // split by comma, but in quotes
        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        rows.add(Row.of(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]), parts[2], parts[3]));
      }
    }
    return rows;
  }

  private FlinkQuickStart() {
  }

  public static void main(String[] args)
      throws Exception {
    // load data
    List<Row> data = loadData();
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(2);
    DataStream<Row> srcDs = execEnv.fromData(data, TEST_TYPE_INFO).keyBy(r -> r.getField(0));

    URI controllerUri = URI.create(DEFAULT_CONTROLLER_URL);
    String controllerAddress = controllerUri.getAuthority();
    String controllerPath = controllerUri.getPath();
    if (controllerPath != null && !controllerPath.isEmpty() && !"/".equals(controllerPath)) {
      controllerAddress += controllerPath.endsWith("/") ? controllerPath.substring(0, controllerPath.length() - 1)
          : controllerPath;
    }
    Properties properties = new Properties();
    properties.setProperty(PinotAdminTransport.ADMIN_TRANSPORT_SCHEME, controllerUri.getScheme());

    try (PinotAdminClient client = new PinotAdminClient(controllerAddress, properties)) {
      Schema schema = client.getSchemaClient().getSchemaObject("starbucksStores");
      TableConfig tableConfig =
          client.getTableClient().getTableConfigObjectForType("starbucksStores", TableType.OFFLINE);
      srcDs.sinkTo(new PinotSink<>(new FlinkRowGenericRowConverter(TEST_TYPE_INFO), tableConfig, schema,
          DEFAULT_CONTROLLER_URL));
    }
    execEnv.execute();
  }
}
