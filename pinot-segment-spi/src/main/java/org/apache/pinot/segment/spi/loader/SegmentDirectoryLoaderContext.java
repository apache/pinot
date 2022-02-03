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
package org.apache.pinot.segment.spi.loader;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Context for {@link SegmentDirectoryLoader}
 */
public class SegmentDirectoryLoaderContext {

  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final String _instanceId;
  private final String _segmentName;
  private final PinotConfiguration _segmentDirectoryConfigs;
  private final String _segmentCrc;

  public SegmentDirectoryLoaderContext(TableConfig tableConfig, Schema schema, String instanceId, String segmentName,
      String segmentCrc, PinotConfiguration segmentDirectoryConfigs) {
    _tableConfig = tableConfig;
    _schema = schema;
    _instanceId = instanceId;
    _segmentName = segmentName;
    _segmentCrc = segmentCrc;
    _segmentDirectoryConfigs = segmentDirectoryConfigs;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public Schema getSchema() {
    return _schema;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getSegmentCrc() {
    return _segmentCrc;
  }

  public PinotConfiguration getSegmentDirectoryConfigs() {
    return _segmentDirectoryConfigs;
  }
}
