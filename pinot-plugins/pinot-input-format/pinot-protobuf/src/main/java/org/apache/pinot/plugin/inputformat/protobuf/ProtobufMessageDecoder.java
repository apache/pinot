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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//TODO: Add support for Schema Registry
public class ProtobufMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufMessageDecoder.class);

  public static final String DESCRIPTOR_FILE_PATH = "descriptorFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";

  private ProtobufRecordExtractor _recordExtractor;
  private String _protoClassName;
  private Message.Builder _builder;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(PROTO_CLASS_NAME),
        "Protocol Buffer schema class name must be provided");

    _protoClassName = props.getOrDefault(PROTO_CLASS_NAME, "");
    Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(_protoClassName,
            props.get(DESCRIPTOR_FILE_PATH));
    _recordExtractor = new ProtobufRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    DynamicMessage dynamicMessage = DynamicMessage.getDefaultInstance(descriptor);
    _builder = dynamicMessage.newBuilderForType();
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    Message message;
    try {
      _builder.mergeFrom(payload);
      message = _builder.build();
    } catch (Exception e) {
      LOGGER.error("Not able to decode protobuf message", e);
      return destination;
    } finally {
      _builder.clear();
    }
    _recordExtractor.extract(message, destination);
    return destination;
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
