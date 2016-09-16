/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.util;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.common.utils.DataTableJavaSerDe;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Custom implementation of {@link com.linkedin.pinot.common.utils.DataTableSerDe} interface.
 * Overrides ser/de methods from {@link DataTableJavaSerDe}
 */
public class DataTableCustomSerDe extends DataTableJavaSerDe {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableCustomSerDe.class);
  private BrokerMetrics _brokerMetrics;

  /**
   * Default constructor for the class.
   */
  public DataTableCustomSerDe() {
    _brokerMetrics = null;
  }

  /**
   * Constructor for the class, registers the broker metrics to emit a metric each time
   * we hit a Java serialization/de-serialization, as the goal is to catch all of them
   * and replace with custom ser/de.
   *
   * @param brokerMetrics Broker metrics
   */
  public DataTableCustomSerDe(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.DATA_TABLE_OBJECT_DESERIALIZATION, 0);
  }

  /**
   *
   * @param object Object to serialize
   * @return Serialized byte-array
   */
  @Override
  public byte[] serialize(Object object) {
    return serializeObject(object);
  }

  /**
   *
   * @param bytes Serialized byte-array
   * @param dataType Enum type of the object
   * @param <T> Type of object
   * @return De-serialized object
   */
  @Override
  public <T extends Serializable> T deserialize(byte[] bytes, DataType dataType) {
    T object = deserializeObject(bytes, dataType);

    if (dataType == DataType.Object) {
      if (_brokerMetrics != null) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.DATA_TABLE_OBJECT_DESERIALIZATION, 1);
      }
      LOGGER.warn("Identified Java serialized object in data table {}", object.getClass().getName());
    }
    return object;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Serializable> T deserializeObject(@Nonnull byte[] bytes, DataType dataType) {
    ByteBuffer byteBuffer;
    switch (dataType) {
      case String:
        try {
          return (T) new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
          LOGGER.error("Exception caught while de-serializing String", e);
          throw new RuntimeException("String de-serialization error", e);
        }

      case MutableLong:
        byteBuffer = ByteBuffer.wrap(bytes);
        return (T) new MutableLongValue(byteBuffer.getLong());

      case Double:
        byteBuffer = ByteBuffer.wrap(bytes);
        return (T) new Double(byteBuffer.getDouble());

      case DoubleArrayList:
        return (T) deserializeDoubleArray(bytes);

      case AvgPair:
        return (T) AvgAggregationFunction.AvgPair.fromBytes(bytes);

      case MinMaxRangePair:
        return (T) MinMaxRangeAggregationFunction.MinMaxRangePair.fromBytes(bytes);

      case HyperLogLog:
        try {
          return (T) HyperLogLog.Builder.build(bytes);
        } catch (IOException e) {
          LOGGER.error("Exception caught while de-serializing HyperLogLog", e);
          throw new RuntimeException("HyperLogLog de-serializing error", e);
        }

      case QuantileDigest:
        return (T) deserializeQuantileDigest(bytes);

      case HashMap:
        return (T) deserializeHashMap(bytes);

      case IntOpenHashSet:
        return (T) deserializeIntOpenHashSet(bytes);

      case Object:
        return (T) deserializeJavaObject(bytes);

      default:
        throw new RuntimeException("Illegal object type found when de-serializing data table");
    }
  }

  /**
   * Serializer for the given object. Uses custom serialization for
   * objects of supported types, and Java serialization
   * for the rest.
   *
   * @param object Object to serialize
   * @return Serialized bytes for the object.
   */
  @SuppressWarnings("unchecked")
  public static byte[] serializeObject(@Nonnull Object object) {

    if (object instanceof String) {
      String value = (String) object;
      try {
        return value.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        LOGGER.error("Exception caught while serializing String", e);
        throw new RuntimeException("String serialization error", e);
      }
    } else if (object instanceof MutableLongValue) {
      MutableLongValue value = (MutableLongValue) object;
      return value.toBytes();

    } else if (object instanceof Double) {
      Double value = (Double) object;
      return serializeDouble(value);

    } else if (object instanceof DoubleArrayList) {
      DoubleArrayList value = (DoubleArrayList) object;
      return serializeDoubleArrayList(value);

    } else if (object instanceof AvgAggregationFunction.AvgPair) {
      AvgAggregationFunction.AvgPair avgPair = (AvgAggregationFunction.AvgPair) object;
      return avgPair.toBytes();

    } else if (object instanceof MinMaxRangeAggregationFunction.MinMaxRangePair) {
      MinMaxRangeAggregationFunction.MinMaxRangePair minMaxRangePair =
          (MinMaxRangeAggregationFunction.MinMaxRangePair) object;
      return minMaxRangePair.toBytes();

    } else if (object instanceof HyperLogLog) {
      try {
        HyperLogLog hll = (HyperLogLog) object;
        return hll.getBytes();
      } catch (IOException e) {
        LOGGER.error("Exception caught while serializing HyperLogLog", e);
        throw new RuntimeException("HyperLogLog serialization error", e);
      }
    } else if (object instanceof QuantileDigest) {
      QuantileDigest quantileDigest = (QuantileDigest) object;
      return serializeQuantileDigest(quantileDigest);

    } else if (object instanceof HashMap) {
      HashMap<Object, Object> map = (HashMap<Object, Object>) object;
      return serializeHashMap(map);

    } else if (object instanceof IntOpenHashSet) {
      IntOpenHashSet hashSet = (IntOpenHashSet) object;
      return serializeIntOpenHashSet(hashSet);

    } else {
      return serializeJavaObject(object);
    }
  }

  /**
   *
   * @param object Object for which to get the data type.
   * @return DataType of the object
   */
  @Override
  public DataType getObjectType(Object object) {
    return getDataTypeOfObject(object);
  }

  /**
   * Helper method to get the data type of the object.
   * @param object Object for which to get the data type
   * @return Data type of the object
   */
  public static DataType getDataTypeOfObject(Object object) {
    if (object instanceof String) {
      return DataType.String;

    } else if (object instanceof MutableLongValue) {
      return DataType.MutableLong;

    } else if (object instanceof Double) {
      return DataType.Double;

    } else if (object instanceof DoubleArrayList) {
      return DataType.DoubleArrayList;

    } else if (object instanceof AvgAggregationFunction.AvgPair) {
      return DataType.AvgPair;

    } else if (object instanceof MinMaxRangeAggregationFunction.MinMaxRangePair) {
      return DataType.MinMaxRangePair;

    } else if (object instanceof HyperLogLog) {
      return DataType.HyperLogLog;

    } else if (object instanceof QuantileDigest) {
      return DataType.QuantileDigest;

    } else if (object instanceof HashMap) {
      return DataType.HashMap;

    } else if (object instanceof IntOpenHashSet) {
      return DataType.IntOpenHashSet;

    } else {
      return DataType.Object;
    }
  }

  /**
   * Helper method to serialize a Double.
   *
   * @param value to serialize
   * @return Serialized byte[] for the double value.
   */
  public static byte[] serializeDouble(Double value) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Double.SIZE >> 3);
    byteBuffer.putDouble(value);
    return byteBuffer.array();
  }

  /**
   * Helper method to serialize a DoubleArray
   *
   * @param doubleArray DoubleArray to serialize
   * @return Serialized value for the doubleArray
   */
  public static byte[] serializeDoubleArrayList(DoubleArrayList doubleArray) {
    int numElements = doubleArray.size();
    int size = (V1Constants.Numbers.INTEGER_SIZE) + (numElements * (V1Constants.Numbers.DOUBLE_SIZE));

    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    byteBuffer.putInt(numElements);

    double[] elements = doubleArray.elements();
    for (int i = 0; i < numElements; i++) {
      byteBuffer.putDouble(elements[i]);
    }
    return byteBuffer.array();
  }

  /**
   * Helper method to de-serialize DoubleArrayList
   *
   * @param bytes Serialized bytes for DoubleArrayList
   * @return De-serialized DoubleArrayList from byte-array.
   */
  public static DoubleArrayList deserializeDoubleArray(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    int numElements = byteBuffer.getInt();
    DoubleArrayList doubleArray = new DoubleArrayList(numElements);

    for (int i = 0; i < numElements; i++) {
      doubleArray.add(byteBuffer.getDouble());
    }
    return doubleArray;
  }

  /**
   * Helper method to serialize a HashMap into a byte-array.
   *
   * @param map Map to serialize
   * @return Serialized byte-array for the HashMap.
   */
  public static byte[] serializeHashMap(HashMap<Object, Object> map) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      // Write the size of the map.
      dataOutputStream.writeInt(map.size());

      // Write the serialized key-value pairs
      boolean first = true;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {

        // Write the key and value type before writing the first key-value pair.
        if (first) {
          dataOutputStream.writeInt(getDataTypeOfObject(entry.getKey()).getValue());
          dataOutputStream.writeInt(getDataTypeOfObject(entry.getValue()).getValue());
          first = false;
        }

        byte[] bytes = serializeObject(entry.getKey());
        dataOutputStream.writeInt(bytes.length);
        dataOutputStream.write(bytes);

        bytes = serializeObject(entry.getValue());
        dataOutputStream.writeInt(bytes.length);
        dataOutputStream.write(bytes);
      }

      dataOutputStream.flush();
      return byteArrayOutputStream.toByteArray();

    } catch (IOException e) {
      LOGGER.error("Exception caught while serializing HashMap", e);
      throw new RuntimeException("HashMap serialization error", e);
    }
  }

  /**
   * Helper method to de-serialize a HashMap
   *
   * @param bytes Serialized bytes for the HashMap
   * @return De-serialized HashMap from the byte-array
   */
  public static Map deserializeHashMap(byte[] bytes) {
    if (bytes.length == 0) {
      return Collections.EMPTY_MAP;
    }
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int size = byteBuffer.getInt();

    HashMap<Object, Object> map = new HashMap<>(size);
    if (size == 0) {
      return map;
    }

    DataType keyType = DataType.valueOf(byteBuffer.getInt());
    DataType valueType = DataType.valueOf(byteBuffer.getInt());

    for (int i = 0; i < size; i++) {
      int numBytes = byteBuffer.getInt();
      byte[] keyBytes = new byte[numBytes];
      byteBuffer.get(keyBytes);
      Object key = deserializeObject(keyBytes, keyType);

      numBytes = byteBuffer.getInt();
      byte[] valueBytes = new byte[numBytes];
      byteBuffer.get(valueBytes);
      Object value = deserializeObject(valueBytes, valueType);

      map.put(key, value);
    }
    return map;
  }

  /**
   * Helper method to serialize QuantileDigest into a byte-array
   *
   * @param quantileDigest to serialize
   * @return Serialized byte-array for the QuantileDigest.
   */
  public static byte[] serializeQuantileDigest(QuantileDigest quantileDigest) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      quantileDigest.serialize(dataOutputStream);
      return byteArrayOutputStream.toByteArray();

    } catch (IOException e) {
      LOGGER.error("Exception caught while serializing QuantileDigest", e);
      throw new RuntimeException("Serialization error for QuantileDigest", e);
    }
  }

  /**
   * Helper method to de-serialize QuantileDigest from a byte-array
   *
   * @param bytes Serialized bytes for the QuantileDigest object
   * @return De-serialized QuantileDigest from the given byte-array
   */
  public static QuantileDigest deserializeQuantileDigest(byte[] bytes) {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);

    try (DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
      return QuantileDigest.deserialize(dataInputStream);

    } catch (IOException e) {
      LOGGER.error("De-serialization error for QuantileDigest", e);
      throw new RuntimeException("QuantileDigest de-serialization error", e);
    }
  }

  /**
   * Helper method to serialize IntOpenHashSet into a byte-array
   *
   * @param set to serialize
   * @return Serialized byte-array for the IntOpenHashSet
   */
  private static byte[] serializeIntOpenHashSet(IntOpenHashSet set) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      // Write the size of the set.
      dataOutputStream.writeInt(set.size());
      for (Integer value : set) {
        dataOutputStream.writeInt(value);
      }
      return byteArrayOutputStream.toByteArray();

    } catch (IOException e) {
      LOGGER.error("Exception caught while serializing IntOpenHashSet", e);
      throw new RuntimeException("HashMap serialization error", e);
    }
  }

  /**
   * Helper method to de-serialize IntOpenHashSet from a byte-array
   *
   * @param bytes Serialized bytes for the IntOpenHashSet
   * @return De-serialized IntOpenHashSet from the given byte-array
   */
  public static IntOpenHashSet deserializeIntOpenHashSet(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int size = byteBuffer.getInt();

    IntOpenHashSet hashSet = new IntOpenHashSet(size);
    for (int i = 0; i < size; i++) {
      hashSet.add(byteBuffer.getInt());
    }
    return hashSet;
  }
}
