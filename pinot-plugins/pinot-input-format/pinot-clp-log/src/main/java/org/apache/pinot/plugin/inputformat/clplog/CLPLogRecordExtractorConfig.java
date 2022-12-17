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
package org.apache.pinot.plugin.inputformat.clplog;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Configuration for the CLPLogRecordExtractor. There is one configuration property:
 * <p></p>
 * <b>fieldsForClpEncoding</b> - A comma-separated list of fields that should be encoded using CLP. Each field encoded
 * by {@link CLPLogRecordExtractor} will result in three output fields prefixed with the original field's name.
 * See {@link CLPLogRecordExtractor} for details. If <b>fieldsForCLPEncoding</b> is empty, no fields will be encoded.
 * <p></p>
 * Each property can be set as part of a table's indexing configuration by adding
 * {@code stream.kafka.decoder.prop.[configurationKeyName]} to {@code streamConfigs}.
 */
public class CLPLogRecordExtractorConfig implements RecordExtractorConfig {
  public static final String FIELDS_FOR_CLP_ENCODING_CONFIG_KEY = "fieldsForClpEncoding";
  public static final String FIELDS_FOR_CLP_ENCODING_SEPARATOR = ",";

  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogRecordExtractorConfig.class);

  private final Set<String> _fieldsForClpEncoding = new HashSet<>();

  @Override
  public void init(Map<String, String> props) {
    RecordExtractorConfig.super.init(props);
    if (null == props) {
      return;
    }

    String concatenatedFieldNames = props.get(FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
    if (null == concatenatedFieldNames) {
      return;
    }
    String[] fieldNames = concatenatedFieldNames.split(FIELDS_FOR_CLP_ENCODING_SEPARATOR);
    for (String fieldName : fieldNames) {
      if (fieldName.isEmpty()) {
        LOGGER.warn("Ignoring empty field name in {}", FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
      } else {
        _fieldsForClpEncoding.add(fieldName);
      }
    }
  }

  public Set<String> getFieldsForClpEncoding() {
    return _fieldsForClpEncoding;
  }
}
