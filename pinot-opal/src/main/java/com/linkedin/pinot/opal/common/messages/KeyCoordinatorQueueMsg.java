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

public class KeyCoordinatorQueueMsg implements Serializable {
  private final String _pinotTable;
  private final byte[] _key;
  private final KeyCoordinatorMessageContext _context;

  public KeyCoordinatorQueueMsg(String pinotTable, byte[] key, KeyCoordinatorMessageContext context) {
    this._pinotTable = pinotTable;
    this._key = key;
    this._context = context;
  }

  public byte[] getKey() {
    return _key;
  }

  public KeyCoordinatorMessageContext getContext() {
    return _context;
  }

  public String getPinotTable() {
    return _pinotTable;
  }

  public static class KeyCoordinatorQueueMsgSerializer implements Serializer<KeyCoordinatorQueueMsg> {
    @Override
    public void configure(Map map, boolean b) { }

    @Override
    public byte[] serialize(String s, KeyCoordinatorQueueMsg o) {
      return SerializationUtils.serialize(o);
    }

    @Override
    public void close() { }
  }

  public static class KeyCoordinatorQueueMsgDeserializer implements Deserializer<KeyCoordinatorQueueMsg> {

    @Override
    public void configure(Map<String, ?> map, boolean b) { }

    @Override
    public KeyCoordinatorQueueMsg deserialize(String s, byte[] bytes) {
      return (KeyCoordinatorQueueMsg) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() { }
  }
}
