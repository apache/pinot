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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of StreamMessageDecoder to read log events from a stream. This is an experimental feature.
 * It allows us to encode user-specified fields of a log event using CLP. See {@link CLPLogRecordExtractor} for more
 * details. The implementation is based on {@link org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder}.
 */
public class CLPLogMessageDecoder implements StreamMessageDecoder<byte[]> {
  public static final String ERROR_SAMPLING_PERIOD_CONFIG_KEY = "errorSamplingPeriod";
  private static final Logger LOGGER = LoggerFactory.getLogger(CLPLogMessageDecoder.class);
  private static final int DEFAULT_ERROR_SAMPLING_PERIOD = 10000;
  private final ServerMetrics _serverMetrics = ServerMetrics.get();

  private RecordExtractor<Map<String, Object>> _recordExtractor;
  // Period at which errors should be sampled for printing:
  // * 0            - Don't print errors
  // * n > 0        - Print one error every n errors
  private int _errorSamplingPeriod = DEFAULT_ERROR_SAMPLING_PERIOD;
  private int _numErrorsUntilNextPrint = 0;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    String recordExtractorClass = null;
    String recordExtractorConfigClass = null;
    String errorSamplingPeriodString = null;
    if (null != props) {
      recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
      recordExtractorConfigClass = props.get(RECORD_EXTRACTOR_CONFIG_CONFIG_KEY);
      errorSamplingPeriodString = props.get(ERROR_SAMPLING_PERIOD_CONFIG_KEY);
    }
    if (null == recordExtractorClass) {
      recordExtractorClass = CLPLogRecordExtractor.class.getName();
      recordExtractorConfigClass = CLPLogRecordExtractorConfig.class.getName();
    }
    _recordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    RecordExtractorConfig config = PluginManager.get().createInstance(recordExtractorConfigClass);
    config.init(props);
    if (_recordExtractor instanceof CLPLogRecordExtractor) {
      ((CLPLogRecordExtractor) _recordExtractor).init(fieldsToRead, config, topicName, _serverMetrics);
    } else {
      _recordExtractor.init(fieldsToRead, config);
    }

    // Parse error sampling period
    if (null != errorSamplingPeriodString) {
      try {
        _errorSamplingPeriod = Integer.parseInt(errorSamplingPeriodString);
      } catch (NumberFormatException e) {
        LOGGER.error("Could not parse errorSamplingPeriod");
      }
      if (_errorSamplingPeriod < 0) {
        LOGGER.error("errorSamplingPeriod cannot be less than 0");
        _errorSamplingPeriod = DEFAULT_ERROR_SAMPLING_PERIOD;
      }
    }
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      JsonNode message = JsonUtils.bytesToJsonNode(payload);
      Map<String, Object> from = JsonUtils.jsonNodeToMap(message);
      _recordExtractor.extract(from, destination);
      return destination;
    } catch (Exception e) {
      if (0 != _errorSamplingPeriod) {
        _numErrorsUntilNextPrint--;
        if (0 == _numErrorsUntilNextPrint) {
          LOGGER.error("Caught exception while decoding row, discarding row. Payload is {}", new String(payload), e);
          _numErrorsUntilNextPrint = _errorSamplingPeriod;
        }
      }
      return null;
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
