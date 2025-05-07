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
package org.apache.pinot.plugin.inputformat.avro;

import java.util.Map;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Config for {@link AvroRecordExtractor}
 */
public class AvroRecordExtractorConfig implements RecordExtractorConfig {
  private boolean _enableLogicalTypes = true;

  @Override
  public void init(Map<String, String> props) {
    _enableLogicalTypes = Boolean.parseBoolean(props.get("enableLogicalTypes"));
  }

  public boolean isEnableLogicalTypes() {
    return _enableLogicalTypes;
  }

  public void setEnableLogicalTypes(boolean enableLogicalTypes) {
    _enableLogicalTypes = enableLogicalTypes;
  }
}
