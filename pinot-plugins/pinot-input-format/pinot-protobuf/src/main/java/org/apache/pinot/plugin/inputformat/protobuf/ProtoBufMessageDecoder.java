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

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//TODO: Add support for Schema Registry
public class ProtoBufMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufMessageDecoder.class);

  public static final String DESCRIPTOR_FILE_PATH = "descriptorFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";

  private ProtoBufRecordExtractor _recordExtractor;
  private String _protoClassName;
  private Message.Builder _builder;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(DESCRIPTOR_FILE_PATH),
        "Protocol Buffer schema descriptor file must be provided");

    _protoClassName = props.getOrDefault(PROTO_CLASS_NAME, "");
    InputStream descriptorFileInputStream = ProtoBufUtils.getDescriptorFileInputStream(
        props.get(DESCRIPTOR_FILE_PATH));
    Descriptors.Descriptor descriptor = buildProtoBufDescriptor(descriptorFileInputStream);
    _recordExtractor = new ProtoBufRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    DynamicMessage dynamicMessage = DynamicMessage.getDefaultInstance(descriptor);
    _builder = dynamicMessage.newBuilderForType();
  }

  private Descriptors.Descriptor buildProtoBufDescriptor(InputStream fin)
      throws IOException {
    try {
      DynamicSchema dynamicSchema = DynamicSchema.parseFrom(fin);

      if (!StringUtils.isEmpty(_protoClassName)) {
        return dynamicSchema.getMessageDescriptor(_protoClassName);
      } else {
        return dynamicSchema.getMessageDescriptor(dynamicSchema.getMessageTypes().toArray(new String[]{})[0]);
      }
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IOException("Descriptor file validation failed", e);
    }
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
