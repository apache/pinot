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
package org.apache.pinot.spi.ingestion.segment.writer;

import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Config to initialize the {@link SegmentWriter}
 */
public class SegmentWriterConfig {
  private final TableConfig _tableConfig;
  private final Schema _schema;

  private SegmentWriterConfig(TableConfig tableConfig, Schema schema) {
    this._tableConfig = tableConfig;
    this._schema = schema;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public Schema getSchema() {
    return _schema;
  }

  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setSchema(Schema schema) {
      _schema = schema;
      return this;
    }

    public SegmentWriterConfig build() {
      Preconditions.checkNotNull(_tableConfig);
      Preconditions.checkNotNull(_schema);
      return new SegmentWriterConfig(_tableConfig, _schema);
    }
  }

  @Override
  public String toString() {
    return "SegmentWriterConfig{" + "\n_tableConfig=" + _tableConfig + ", \n_schema=" + _schema.toSingleLineJsonString()
        + "\n}";
  }
}
