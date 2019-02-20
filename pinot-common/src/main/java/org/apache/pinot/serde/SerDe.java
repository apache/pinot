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
package org.apache.pinot.serde;

import javax.annotation.concurrent.NotThreadSafe;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thrift based serialization-deserialization.
 *
 * Note: We did not make this a serialization protocol (thrift, kryo, protobuf...) agnostic  interface
 * as the underlying requests/response is itself specific to protocol (thrift vs protobuf). It does not
 * make sense to encapsulate the serialization logic from it. In future if you want to move away from thrift,
 * you would have to regenerate (or redefine) request/response classes anyways and implement the SerDe for it.
 *
 * Please note the implementation is not thread-safe as underlying thrift serialization is not threadsafe.
 *
 */
@NotThreadSafe
public class SerDe {
  protected static final Logger LOGGER = LoggerFactory.getLogger(SerDe.class);

  private final TSerializer _serializer;
  private final TDeserializer _deserializer;

  public SerDe(TProtocolFactory factory) {
    _serializer = new TSerializer(factory);
    _deserializer = new TDeserializer(factory);
  }

  public byte[] serialize(@SuppressWarnings("rawtypes") TBase obj) {
    try {
      return _serializer.serialize(obj);
    } catch (TException e) {
      LOGGER.error("Unable to serialize object :" + obj, e);
      return null;
    }
  }

  public boolean deserialize(@SuppressWarnings("rawtypes") TBase obj, byte[] payload) {
    try {
      _deserializer.deserialize(obj, payload);
    } catch (TException e) {
      LOGGER.error("Unable to deserialize to object :" + obj, e);
      return false;
    }
    return true;
  }
}
