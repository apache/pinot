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

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;


public class FileBasedStreamProviderImpl implements StreamProvider {

  private FileBasedStreamProviderConfig config;
  private RecordReader reader;
  private int count;

  @Override
  public void init(StreamProviderConfig streamProviderConfig, String tableName, ServerMetrics serverMetrics)
      throws Exception {
    config = (FileBasedStreamProviderConfig) streamProviderConfig;

    FieldExtractor extractor = FieldExtractorFactory.getPlainFieldExtractor(config.getSchema());
    reader = RecordReaderFactory.get(this.config.getFormat(), this.config.getPath(), extractor);
    reader.init();
    count = 0;
  }

  @Override
  public void start() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GenericRow next(GenericRow destination) {
    if (reader.hasNext()) {
      count++;
      return reader.next();
    }

    return null;
  }

  @Override
  public GenericRow next(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long currentOffset() {
    return count;
  }

  @Override
  public void commit() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() throws Exception {
    reader.close();
  }

}
