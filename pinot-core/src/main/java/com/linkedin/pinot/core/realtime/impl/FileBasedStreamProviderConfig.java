/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import java.util.Map;


public class FileBasedStreamProviderConfig implements StreamProviderConfig {

  private FileFormat format;
  private String path;
  private Schema schema;

  public FileBasedStreamProviderConfig(FileFormat format, String path, Schema schema) {
    this.format = format;
    this.path = path;
    this.schema = schema;
  }

  public FileFormat getFormat() {
    return format;
  }

  public void setFormat(FileFormat format) {
    this.format = format;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init(Map<String, String> properties, Schema schema) {
    // TODO Auto-generated method stub
  }

  @Override
  public String getStreamProviderClass() {
    return null;
  }

  @Override
  public void init(TableConfig tableConfig, InstanceZKMetadata instanceMetadata, Schema schema) {
  }

  @Override
  public int getSizeThresholdToFlushSegment() {
    return 100000;
  }

  @Override
  public long getTimeThresholdToFlushSegment() {
    // TODO Auto-generated method stub
    return 1000 * 60 * 60;
  }

  @Override
  public String getStreamName() {
    return getPath();
  }
}
