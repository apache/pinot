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
  private final String _segmentCrc;
  private final PinotConfiguration _segmentDirectoryConfigs;

  private SegmentDirectoryLoaderContext(TableConfig tableConfig, Schema schema, String instanceId, String segmentName,
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

  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;
    private String _instanceId;
    private String _segmentName;
    private String _segmentCrc;
    private PinotConfiguration _segmentDirectoryConfigs;

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setSchema(Schema schema) {
      _schema = schema;
      return this;
    }

    public Builder setInstanceId(String instanceId) {
      _instanceId = instanceId;
      return this;
    }

    public Builder setSegmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder setSegmentCrc(String segmentCrc) {
      _segmentCrc = segmentCrc;
      return this;
    }

    public Builder setSegmentDirectoryConfigs(PinotConfiguration segmentDirectoryConfigs) {
      _segmentDirectoryConfigs = segmentDirectoryConfigs;
      return this;
    }

    public SegmentDirectoryLoaderContext build() {
      return new SegmentDirectoryLoaderContext(_tableConfig, _schema, _instanceId, _segmentName, _segmentCrc,
          _segmentDirectoryConfigs);
    }
  }
}
