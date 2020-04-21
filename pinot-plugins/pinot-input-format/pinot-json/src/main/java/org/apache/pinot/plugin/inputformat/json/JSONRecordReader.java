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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Record reader for JSON file.
 */
public class JSONRecordReader implements RecordReader<Map<String, Object>> {
  private File _dataFile;
  private Schema _schema;

  private MappingIterator<Map<String, Object>> _iterator;

  public JSONRecordReader() {
  }

  private void init()
      throws IOException {
    try {
      _iterator = JsonUtils.DEFAULT_READER.forType(new TypeReference<Map<String, Object>>() {
      }).readValues(_dataFile);
    } catch (Exception e) {
      if (_iterator != null) {
        _iterator.close();
      }
      throw e;
    }
  }

  @Override
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    _schema = schema;

    init();
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  // NOTE: hard to extract common code further
  @SuppressWarnings("Duplicates")
  @Override
  public Map<String, Object> next() {
    return _iterator.next();
  }

  @Override
  public Map<String, Object> next(Map<String, Object> reuse) {
    reuse.putAll(_iterator.next());
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _iterator.close();
    init();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close()
      throws IOException {
    _iterator.close();
  }
}
