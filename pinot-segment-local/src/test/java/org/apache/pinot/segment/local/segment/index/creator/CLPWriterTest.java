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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.segment.local.io.writer.impl.CLPForwardIndexWriterV1;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.readers.forward.CLPForwardIndexReaderV1;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;


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
public class CLPWriterTest {

  @Test
  public void testCLPWriter()
      throws Exception {
    List<String> logLines = new ArrayList<>();
    logLines.add(
        "2023/10/26 00:03:10.168 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32c_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32c_DEFAULT : Refreshed 35 property LiveInstance took 5 ms. Selective: true");
    logLines.add(
        "2023/10/26 00:03:10.169 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32d_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32d_DEFAULT : Refreshed 81 property LiveInstance took 4 ms. Selective: true");
    logLines.add(
        "2023/10/27 16:35:10.470 INFO [ControllerResponseFilter] [grizzly-http-server-2] Handled request from 10.12"
            + ".15.1 GET https://10.12.15.10:8443/health?checkType=liveness, content-type null status code 200 OK");
    logLines.add(
        "2023/10/27 16:35:10.607 INFO [ControllerResponseFilter] [grizzly-http-server-6] Handled request from 10.12"
            + ".19.5 GET https://pinot-pinot-broker-headless.managed.svc.cluster.local:8093/tables, content-type "
            + "application/json status code 200 OK");

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec("column1", FieldSpec.DataType.STRING, true));
    TableConfig tableConfig =
        new TableConfig("mytable", TableType.REALTIME.name(), new SegmentsValidationAndRetentionConfig(),
            new TenantConfig(null, null, null), new IndexingConfig(), new TableCustomConfig(null), null, null, null,
            null, null, null, null, null, null, null, null, false, null, null, null);
    List<FieldConfig> fieldConfigList = new ArrayList<>();
    fieldConfigList.add(new FieldConfig("column1", FieldConfig.EncodingType.RAW, Collections.EMPTY_LIST,
        FieldConfig.CompressionCodec.CLP, Collections.EMPTY_MAP));
    tableConfig.setFieldConfigList(fieldConfigList);
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(tableConfig, schema, null);
    StringColumnPreIndexStatsCollector statsCollector =
        new StringColumnPreIndexStatsCollector("column1", statsCollectorConfig);
    for (String logLine : logLines) {
      statsCollector.collect(logLine);
    }

    statsCollector.seal();
    System.out.println(statsCollector.getCLPStats());

    File tmpDir = new File("/tmp/");
    File indexFile = new File(tmpDir, "column1.fwd");
    CLPForwardIndexWriterV1 clpForwardIndexWriterV1 =
        new CLPForwardIndexWriterV1(tmpDir, indexFile, "column1", 4, statsCollector);

    for (String logLine : logLines) {
      clpForwardIndexWriterV1.putString(logLine);
    }
    clpForwardIndexWriterV1.close();

    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    CLPForwardIndexReaderV1 clpForwardIndexReaderV1 = new CLPForwardIndexReaderV1(pinotDataBuffer, logLines.size());
    for (int i = 0; i < logLines.size(); i++) {
      System.out.println(clpForwardIndexReaderV1.getString(i, clpForwardIndexReaderV1.createContext()));
    }
  }
}
