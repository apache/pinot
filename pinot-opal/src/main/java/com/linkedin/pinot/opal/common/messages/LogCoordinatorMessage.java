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
package com.linkedin.pinot.opal.common.messages;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class LogCoordinatorMessage implements Serializable {
  private final String _tableName;
  private final String _segmentName;
  private final long _newOffset;
  private final long _oldOffset;
  private final LogEventType _updateEventType;
  private long _kafkaOffset;

  public String getTableName() {
    return _tableName;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public long getOldOffset() {
    return _oldOffset;
  }

  public long getNewOffset() {
    return _newOffset;
  }

  public LogEventType getUpdateEventType() {
    return _updateEventType;
  }

  public byte[] toBytes() {
    return SerializationUtils.serialize(this);
  }

  public long getKafkaOffset() {
    return _kafkaOffset;
  }

  public LogCoordinatorMessage(String tableName, String segmentName, long oldOffset,
                               long newOffset, LogEventType updateEventType) {
    this._tableName = tableName;
    this._segmentName = segmentName;
    this._oldOffset = oldOffset;
    this._newOffset= newOffset;
    this._updateEventType = updateEventType;
    this._kafkaOffset = 0;
  }

  public LogCoordinatorMessage withKafkaOffset(long offset) {
    this._kafkaOffset = offset;
    return this;
  }

  public static class LogCoordinatorMessageSerializer implements Serializer<LogCoordinatorMessage> {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, LogCoordinatorMessage o) {
      return SerializationUtils.serialize(o);
    }

    @Override
    public void close() {
    }
  }

  public static class LogCoordinatorMessageDeserializer implements Deserializer<LogCoordinatorMessage> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public LogCoordinatorMessage deserialize(String s, byte[] bytes) {
      return (LogCoordinatorMessage) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {
    }
  }
}
