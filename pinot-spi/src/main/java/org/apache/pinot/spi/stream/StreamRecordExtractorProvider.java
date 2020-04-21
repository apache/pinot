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
package org.apache.pinot.spi.stream;

import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.SchemaFieldExtractorUtils;


/**
 * Provider for {@link StreamMessageDecoder}
 */
public abstract class StreamRecordExtractorProvider {

  private static final String RECORD_EXTRACTOR_CONFIG_KEY = "recordExtractorClass";

  /**
   * Constructs a RecordExtractor using properties in {@link StreamConfig} and initializes it
   */
  public static RecordExtractor create(StreamMessageDecoder decoder, Map<String, String> decoderProperties, Schema schema) {
    RecordExtractor recordExtractor = null;
    String recordExtractorClass = decoderProperties.get(RECORD_EXTRACTOR_CONFIG_KEY);
    if (recordExtractorClass != null) {
      try {
        recordExtractor = PluginManager.get().createInstance(recordExtractorClass);
      } catch (Exception e) {

      }
    }

    Set<String> sourceFields = SchemaFieldExtractorUtils.extract(schema);
    recordExtractor.init(sourceFields, null);
    return recordExtractor;
  }
}
