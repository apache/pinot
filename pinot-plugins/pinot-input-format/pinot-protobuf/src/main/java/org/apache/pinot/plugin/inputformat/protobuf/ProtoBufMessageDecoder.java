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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.ProtoBufDescriptorFallbackListener;


//TODO: Add support for Schema Registry
public class ProtoBufMessageDecoder implements StreamMessageDecoder<byte[]> {
  public static final String DESCRIPTOR_FILE_PATH = "descriptorFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";
  /// When enabled, a remote descriptor that both fetched and resolved successfully before is served when the
  /// remote fetch fails, so a transient DNS / object-store outage does not permanently fail the CONSUMING
  /// transition. When this decoder prop is not set, the cluster config
  /// 'pinot.server.protobuf.descriptor.fallback.enabled' applies (enabled by default, dynamically updatable);
  /// setting the prop to 'true' or 'false' overrides the cluster-wide value for the table.
  public static final String DESCRIPTOR_FILE_FALLBACK_ENABLED = "descriptorFileFallbackEnabled";

  private ProtoBufRecordExtractor _recordExtractor;
  private Message.Builder _builder;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(DESCRIPTOR_FILE_PATH),
        "Protocol Buffer schema descriptor file must be provided");

    Descriptors.Descriptor descriptor = ProtoBufUtils.getDescriptor(props.get(DESCRIPTOR_FILE_PATH),
        props.getOrDefault(PROTO_CLASS_NAME, ""), isDescriptorFallbackEnabled(props));
    _recordExtractor = new ProtoBufRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    DynamicMessage dynamicMessage = DynamicMessage.getDefaultInstance(descriptor);
    _builder = dynamicMessage.newBuilderForType();
  }

  /// The table-level decoder prop, when set, overrides the dynamically updatable cluster-wide setting.
  @VisibleForTesting
  static boolean isDescriptorFallbackEnabled(Map<String, String> props) {
    String tableOverride = props.get(DESCRIPTOR_FILE_FALLBACK_ENABLED);
    return tableOverride != null ? Boolean.parseBoolean(tableOverride)
        : ProtoBufDescriptorFallbackListener.getInstance().isEnabled();
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    return decode(payload, 0, payload.length, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    Message message;
    try {
      message = _builder.mergeFrom(payload, offset, length).build();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while decoding protobuf message", e);
    } finally {
      _builder.clear();
    }
    return _recordExtractor.extract(message, destination);
  }
}
