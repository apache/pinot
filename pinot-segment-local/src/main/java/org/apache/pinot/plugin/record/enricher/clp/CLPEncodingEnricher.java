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
package org.apache.pinot.plugin.record.enricher.clp;

import com.fasterxml.jackson.databind.JsonNode;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordenricher.RecordEnricher;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.rewriter.ClpRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Enriches the record with CLP encoded fields.
 * For a column 'x', it adds three new columns to the record:
 * 1. 'x_logtype' - The logtype of the encoded message
 * 2. 'x_dictVars' - The dictionary variables of the encoded message
 * 3. 'x_encodedVars' - The encoded variables of the encoded message
 */
public class CLPEncodingEnricher implements RecordEnricher {
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPEncodingEnricher.class);
  private final ClpEnricherConfig _config;
  private final EncodedMessage _clpEncodedMessage;
  private final MessageEncoder _clpMessageEncoder;

  public CLPEncodingEnricher(JsonNode enricherProperties) throws IOException {
    _config = JsonUtils.jsonNodeToObject(enricherProperties, ClpEnricherConfig.class);
    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
  }

  @Override
  public List<String> getInputColumns() {
    return _config.getFields();
  }

  @Override
  public void enrich(GenericRow record) {
    try {
      for (String field : _config.getFields()) {
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
      if (value instanceof String) {
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
      } else {
        LOGGER.error("Can't encode value of type {} with CLP. name: '{}', value: '{}'",
            value.getClass().getSimpleName(), key, value);
      }
    }

    to.putValue(key + ClpRewriter.LOGTYPE_COLUMN_SUFFIX, logtype);
    to.putValue(key + ClpRewriter.DICTIONARY_VARS_COLUMN_SUFFIX, dictVars);
    to.putValue(key + ClpRewriter.ENCODED_VARS_COLUMN_SUFFIX, encodedVars);
  }
}
