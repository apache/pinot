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
package org.apache.pinot.plugin.minion.executor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.minion.PinotTaskConfig;


/**
 * The class <code>SegmentConversionResult</code> wraps the result of
 * {@link BaseSingleSegmentConversionExecutor#convert(PinotTaskConfig, File, File)}.
 */
public class SegmentConversionResult {
  private final File _file;
  private final String _tableNameWithType;
  private final String _segmentName;
  private final Map<String, Object> _customProperties;

  private SegmentConversionResult(File file, String tableNameWithType, String segmentName,
      Map<String, Object> customProperties) {
    _file = file;
    _tableNameWithType = tableNameWithType;
    _segmentName = segmentName;
    _customProperties = customProperties;
  }

  public File getFile() {
    return _file;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  @SuppressWarnings("unchecked")
  public <T> T getCustomProperty(String key) {
    return (T) _customProperties.get(key);
  }

  public static class Builder {
    private File _file;
    private String _tableNameWithType;
    private String _segmentName;
    private final Map<String, Object> _customProperties = new HashMap<>();

    public Builder setFile(File file) {
      _file = file;
      return this;
    }

    public Builder setTableNameWithType(String tableNameWithType) {
      _tableNameWithType = tableNameWithType;
      return this;
    }

    public Builder setSegmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder setCustomProperty(String key, Object property) {
      _customProperties.put(key, property);
      return this;
    }

    public SegmentConversionResult build() {
      return new SegmentConversionResult(_file, _tableNameWithType, _segmentName, _customProperties);
    }
  }
}
