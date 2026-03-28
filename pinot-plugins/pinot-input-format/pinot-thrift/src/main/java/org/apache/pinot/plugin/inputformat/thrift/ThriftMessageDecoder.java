/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pinot.plugin.inputformat.thrift;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** An implementation of StreamMessageDecoder to read Thrift records from a stream. */
public class ThriftMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftMessageDecoder.class);
  private static final ThreadLocal<TDeserializer> DESERIALIZER_COMPACT =
      new ThreadLocal<TDeserializer>() {
        @Override
        protected TDeserializer initialValue() {
          return new TDeserializer(new TCompactProtocol.Factory());
        }
      };

  private static final ThreadLocal<TDeserializer> DESERIALIZER_BINARY =
      new ThreadLocal<TDeserializer>() {
        @Override
        protected TDeserializer initialValue() {
          return new TDeserializer(new TBinaryProtocol.Factory());
        }
      };

  private static final ThreadLocal<TDeserializer> DESERIALIZER_JSON =
      new ThreadLocal<TDeserializer>() {
        @Override
        protected TDeserializer initialValue() {
          return new TDeserializer(new TJSONProtocol.Factory());
        }
      };

  private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final String THRIFT_CLASS_PROP_KEY = "thrift.class";
  private static final String THRIFT_RECORD_EXTRACTOR_CLASS =
      "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordExtractor";

  private RecordExtractor<TBase> _thriftRecordExtractor;
  private Class<TBase> _thriftClass;

  public Class<TBase> getThriftClass(String thriftClassName)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, InvocationTargetException {
    return (Class<TBase>) Class.forName(thriftClassName).getConstructor().newInstance();
  }

  public static byte[] decodeB64IfNeeded(final byte[] src) {
    checkNotNull(src, "src bytes cannot be null");
    if (src.length <= 0) {
      return EMPTY_BYTES;
    }
    final byte last = src[src.length - 1];
    return (0 == last || '}' == last) ? src : BASE64_DECODER.decode(src);
  }

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    checkState(
        props.containsKey(THRIFT_CLASS_PROP_KEY),
        "The " + THRIFT_CLASS_PROP_KEY + " must be provided for streaming thrift records");
    _thriftClass = getThriftClass(props.get(THRIFT_CLASS_PROP_KEY));
    String recordExtractorClass = null;
    if (props != null) {
      recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
    }
    if (recordExtractorClass == null) {
      recordExtractorClass = THRIFT_RECORD_EXTRACTOR_CLASS;
    }
    _thriftRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    _thriftRecordExtractor.init(fieldsToRead, null);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      final byte[] src = decodeB64IfNeeded(payload);
      final TProtocolFactory protocolFactory = TProtocolUtil.guessProtocolFactory(src, null);
      checkNotNull(protocolFactory);
      final TBase from = _thriftClass.newInstance();
      if (protocolFactory instanceof TCompactProtocol.Factory) {
        DESERIALIZER_COMPACT.get().deserialize(from, src);
      } else if (protocolFactory instanceof TBinaryProtocol.Factory) {
        DESERIALIZER_BINARY.get().deserialize(from, src);
      } else {
        DESERIALIZER_JSON.get().deserialize(from, src);
      }
      _thriftRecordExtractor.extract(from, destination);
      return destination;
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while decoding row, discarding row. Payload is {}",
          new String(payload),
          e);
      return null;
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
