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
package org.apache.pinot.segment.local.recordenricher.clp;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.recordenricher.RecordEnricher;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.sql.parsers.rewriter.CLPDecodeRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CLPEncodingEnricher extends RecordEnricher {
  public static final String FIELDS_FOR_CLP_ENCODING_CONFIG_KEY = "fieldsForClpEncoding";
  public static final String FIELDS_FOR_CLP_ENCODING_SEPARATOR = ",";
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPEncodingEnricher.class);

  private List<String> _fields;
  private EncodedMessage _clpEncodedMessage;
  private MessageEncoder _clpMessageEncoder;

  @Override
  public void init(Map<String, String> enricherProperties) {
    String concatenatedFieldNames = enricherProperties.get(FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
    if (StringUtils.isEmpty(concatenatedFieldNames)) {
      throw new IllegalArgumentException("Missing required property: " + FIELDS_FOR_CLP_ENCODING_CONFIG_KEY);
    } else {
      _fields = List.of(concatenatedFieldNames.split(FIELDS_FOR_CLP_ENCODING_SEPARATOR));
    }

    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
  }

  @Override
  public List<String> getInputColumns() {
    return _fields;
  }

  @Override
  public void enrich(GenericRow record) {
    try {
      for (String field : _fields) {
        Object value = record.getValue(field);
        if (value != null) {
          enrichWithClpEncodedFields(field, value, record);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to enrich record: {}", record);
    }
  }

  private void enrichWithClpEncodedFields(String key, Object value, GenericRow to) {
    String logtype = null;
    Object[] dictVars = null;
    Object[] encodedVars = null;
    if (null != value) {
      if (!(value instanceof String)) {
        LOGGER.error("Can't encode value of type {} with CLP. name: '{}', value: '{}'",
            value.getClass().getSimpleName(), key, value);
      } else {
        String valueAsString = (String) value;
        try {
          _clpMessageEncoder.encodeMessage(valueAsString, _clpEncodedMessage);
          logtype = _clpEncodedMessage.getLogTypeAsString();
          encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
          dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
        } catch (IOException e) {
          LOGGER.error("Can't encode field with CLP. name: '{}', value: '{}', error: {}", key, valueAsString,
              e.getMessage());
        }
      }
    }

    to.putValue(key + CLPDecodeRewriter.LOGTYPE_COLUMN_SUFFIX, logtype);
    to.putValue(key + CLPDecodeRewriter.DICTIONARY_VARS_COLUMN_SUFFIX, dictVars);
    to.putValue(key + CLPDecodeRewriter.ENCODED_VARS_COLUMN_SUFFIX, encodedVars);
  }
}
