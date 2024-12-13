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
 * Configuration for the CLPLogRecordExtractor. It contains the following properties:
 * <p></p>
 * <ul>
 *   <li><b>fieldsForClpEncoding</b> - A comma-separated list of fields that should be encoded using CLP. Each field
 *   encoded by {@link CLPLogRecordExtractor} will result in three output fields prefixed with the original field's
 *   name. See {@link CLPLogRecordExtractor} for details. If <b>fieldsForCLPEncoding</b> is empty, no fields will be
 *   encoded.</li>
 *   <li><b>unencodableFieldSuffix</b> - A suffix to apply to fields that could not be encoded with CLP.</li>
 *   <li><b>unencodableFieldError</b> - An error message to replace the field's original value so that users know that
 *   there was a problem encoding their field.</li>
 * </ul>
 *
 * <p></p>
 * Each property can be set as part of a table's indexing configuration by adding
 * {@code stream.kafka.decoder.prop.[configurationKeyName]} to {@code streamConfigs}.
 */
public class CLPLogRecordExtractorConfig implements RecordExtractorConfig {
  public static final String FIELDS_FOR_CLP_ENCODING_CONFIG_KEY = "fieldsForClpEncoding";
  public static final String FIELDS_FOR_CLP_ENCODING_SEPARATOR = ",";
  public static final String REMOVE_PROCESSED_FIELDS_CONFIG_KEY = "removeProcessedFields";
  public static final String UNENCODABLE_FIELD_SUFFIX_CONFIG_KEY = "unencodableFieldSuffix";
  public static final String UNENCODABLE_FIELD_ERROR_CONFIG_KEY = "unencodableFieldError";

  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogRecordExtractorConfig.class);

  private final Set<String> _fieldsForClpEncoding = new HashSet<>();
  private String _unencodableFieldSuffix = null;
  private String _unencodableFieldError = null;
  private boolean _removeProcessedFields = false;

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

    // True if field is specified and equal to true (ignore case), false for all other cases
    _removeProcessedFields = Boolean.parseBoolean(props.get(REMOVE_PROCESSED_FIELDS_CONFIG_KEY));

    String unencodableFieldSuffix = props.get(UNENCODABLE_FIELD_SUFFIX_CONFIG_KEY);
    if (null != unencodableFieldSuffix) {
      if (unencodableFieldSuffix.length() == 0) {
        LOGGER.warn("Ignoring empty value for {}", UNENCODABLE_FIELD_SUFFIX_CONFIG_KEY);
      } else {
        _unencodableFieldSuffix = unencodableFieldSuffix;
      }
    }

    String unencodableFieldError = props.get(UNENCODABLE_FIELD_ERROR_CONFIG_KEY);
    if (null != unencodableFieldError) {
      if (unencodableFieldError.length() == 0) {
        LOGGER.warn("Ignoring empty value for {}", UNENCODABLE_FIELD_ERROR_CONFIG_KEY);
      } else {
        _unencodableFieldError = unencodableFieldError;
      }
    }
  }

  public Set<String> getFieldsForClpEncoding() {
    return _fieldsForClpEncoding;
  }

  public boolean getRemoveProcessedFields() {
    return _removeProcessedFields;
  }

  public String getUnencodableFieldSuffix() {
    return _unencodableFieldSuffix;
  }

  public String getUnencodableFieldError() {
    return _unencodableFieldError;
  }
}
