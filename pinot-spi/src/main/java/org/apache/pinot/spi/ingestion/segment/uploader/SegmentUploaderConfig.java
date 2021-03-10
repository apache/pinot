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
package org.apache.pinot.spi.ingestion.segment.uploader;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;


/**
 * Config to initialize the {@link SegmentUploader}
 */
public class SegmentUploaderConfig {
  private final TableConfig _tableConfig;
  private final BatchConfig _batchConfigOverride;

  private SegmentUploaderConfig(TableConfig tableConfig, @Nullable BatchConfig batchConfigOverride) {
    this._tableConfig = tableConfig;
    this._batchConfigOverride = batchConfigOverride;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  @Nullable
  public BatchConfig getBatchConfigOverride() {
    return _batchConfigOverride;
  }

  public static class Builder {
    private TableConfig _tableConfig;
    private BatchConfig _batchConfigOverride;

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setBatchConfigOverride(@Nullable BatchConfig batchConfigOverride) {
      _batchConfigOverride = batchConfigOverride;
      return this;
    }

    public SegmentUploaderConfig build() {
      Preconditions.checkNotNull(_tableConfig);
      return new SegmentUploaderConfig(_tableConfig, _batchConfigOverride);
    }
  }

  @Override
  public String toString() {
    return "SegmentWriterConfig{" + "\n_tableConfig=" + _tableConfig + ", \nbatchConfigOverride=" + _batchConfigOverride
        + "\n}";
  }
}
