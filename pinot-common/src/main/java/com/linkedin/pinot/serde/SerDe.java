package com.linkedin.pinot.serde;

import org.apache.http.annotation.NotThreadSafe;
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
 * @author bvaradar
 *
 */
@NotThreadSafe
public class SerDe {
  protected static Logger LOG = LoggerFactory.getLogger(SerDe.class);  
  
  private final TSerializer _serializer;
  private final TDeserializer _deserializer;
  
  public SerDe(TProtocolFactory factory)
  {
    _serializer = new TSerializer(factory);
    _deserializer = new TDeserializer(factory);
  }
  
 
  public byte[] serialize(@SuppressWarnings("rawtypes") TBase obj)
  {
    try {
      return _serializer.serialize(obj);
    } catch (TException e) {
      LOG.error("Unable to serialize object :" + obj, e);
      return null;
    }
  }
  
  public boolean deserialize(@SuppressWarnings("rawtypes") TBase obj, byte[] payload)
  {
    try {
      _deserializer.deserialize(obj,payload);
    } catch (TException e) {
      LOG.error("Unable to deserialize to object :" + obj, e);
      return false;
    }
    return true;
  }
}
