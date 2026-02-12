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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.ConfigRecord;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Bridge utility for converting between {@link TableConfig} and {@link ZNRecord}.
 * Delegates to {@link TableConfig#fromConfigRecord(ConfigRecord)} and
 * {@link TableConfig#toConfigRecord()} for the actual serialization logic.
 */
public class TableConfigSerDeUtils {
  private TableConfigSerDeUtils() {
  }

  public static TableConfig fromZNRecord(ZNRecord znRecord)
      throws IOException {
    ConfigRecord record = new ConfigRecord(
        znRecord.getId(), znRecord.getSimpleFields(), znRecord.getMapFields());
    return TableConfig.fromConfigRecord(record);
  }

  public static ZNRecord toZNRecord(TableConfig tableConfig)
      throws JsonProcessingException {
    ConfigRecord record = tableConfig.toConfigRecord();
    ZNRecord znRecord = new ZNRecord(record.getId());
    znRecord.setSimpleFields(new HashMap<>(record.getSimpleFields()));
    for (Map.Entry<String, Map<String, String>> entry : record.getMapFields().entrySet()) {
      znRecord.setMapField(entry.getKey(), new HashMap<>(entry.getValue()));
    }
    return znRecord;
  }
}
