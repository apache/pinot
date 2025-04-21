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
package org.apache.pinot.common.datatable;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * A map that stores statistics.
 * <p>
 * Statistics must be keyed by an enum that implements {@link StatMap.Key}.
 * <p>
 * A stat map efficiently store, serialize and deserialize these statistics.
 * <p>
 * Serialization and deserialization is backward and forward compatible as long as the only change in the keys are:
 * <ul>
 *   <li>Adding new keys</li>
 *   <li>Change the name of the keys</li>
 * </ul>
 *
 * Any other change (like changing the type of key, changing their literal order are not supported or removing keys)
 * are backward incompatible changes.
 * @param <K>
 */
public class StatMap<K extends Enum<K> & StatMap.Key> {
  private final Class<K> _keyClass;
  private final Map<K, Object> _map;

  private static final ConcurrentHashMap<Class<?>, Object[]> KEYS_BY_CLASS = new ConcurrentHashMap<>();

  public StatMap(Class<K> keyClass) {
    _keyClass = keyClass;
    // TODO: Study whether this is fine or we should impose a single thread policy in StatMaps
    _map = Collections.synchronizedMap(new EnumMap<>(keyClass));
  }

  /// A copy constructor for the class.
  /// @param other The object to copy, which will be deep copied.
  public StatMap(StatMap<K> other) {
    this(other._keyClass);
    _map.putAll(other._map);
  }

  public int getInt(K key) {
    Preconditions.checkArgument(key.getType() == Type.INT, "Key %s is of type %s, not INT", key, key.getType());
    Object o = _map.get(key);
    return o == null ? 0 : (Integer) o;
  }

  public StatMap<K> merge(K key, int value) {
    if (key.getType() == Type.LONG) {
      merge(key, (long) value);
      return this;
    }
    int oldValue = getInt(key);
    int newValue = key.merge(oldValue, value);
    if (newValue == 0) {
      _map.remove(key);
    } else {
      _map.put(key, newValue);
    }
    return this;
  }

  public long getLong(K key) {
    if (key.getType() == Type.INT) {
      return getInt(key);
    }
    Preconditions.checkArgument(key.getType() == Type.LONG, "Key %s is of type %s, not LONG", key, key.getType());
    Object o = _map.get(key);
    return o == null ? 0L : (Long) o;
  }

  public StatMap<K> merge(K key, long value) {
    Preconditions.checkArgument(key.getType() == Type.LONG, "Key %s is of type %s, not LONG", key, key.getType());
    long oldValue = getLong(key);
    long newValue = key.merge(oldValue, value);
    if (newValue == 0) {
      _map.remove(key);
    } else {
      _map.put(key, newValue);
    }
    return this;
  }

  public boolean getBoolean(K key) {
    Preconditions.checkArgument(key.getType() == Type.BOOLEAN, "Key %s is of type %s, not BOOLEAN",
        key, key.getType());
    Object o = _map.get(key);
    return o != null && (Boolean) o;
  }

  public StatMap<K> merge(K key, boolean value) {
    boolean oldValue = getBoolean(key);
    boolean newValue = key.merge(oldValue, value);
    if (!newValue) {
      _map.remove(key);
    } else {
      _map.put(key, Boolean.TRUE);
    }
    return this;
  }

  public String getString(K key) {
    Preconditions.checkArgument(key.getType() == Type.STRING, "Key %s is of type %s, not STRING", key, key.getType());
    Object o = _map.get(key);
    return o == null ? null : (String) o;
  }

  public StatMap<K> merge(K key, String value) {
    String oldValue = getString(key);
    String newValue = key.merge(oldValue, value);
    if (newValue == null) {
      _map.remove(key);
    } else {
      _map.put(key, newValue);
    }
    return this;
  }

  /**
   * Returns the value associated with the key.
   * <p>
   * Primitives will be boxed, so it is recommended to use the specific methods for each type.
   */
  public Object getAny(K key) {
    switch (key.getType()) {
      case BOOLEAN:
        return getBoolean(key);
      case INT:
        return getInt(key);
      case LONG:
        return getLong(key);
      case STRING:
        return getString(key);
      default:
        throw new IllegalArgumentException("Unsupported type: " + key.getType());
    }
  }

  /**
   * Modifies this object to merge the values of the other object.
   *
   * @param other The object to merge with. This argument will not be modified.
   * @return this object once it is modified.
   */
  public StatMap<K> merge(StatMap<K> other) {
    Preconditions.checkState(_keyClass.equals(other._keyClass),
        "Different key classes %s and %s", _keyClass, other._keyClass);
    for (Map.Entry<K, Object> entry : other._map.entrySet()) {
      K key = entry.getKey();
      Object value = entry.getValue();
      if (value == null) {
        continue;
      }
      switch (key.getType()) {
        case BOOLEAN:
          merge(key, (boolean) value);
          break;
        case INT:
          merge(key, (int) value);
          break;
        case LONG:
          merge(key, (long) value);
          break;
        case STRING:
          merge(key, (String) value);
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + key.getType());
      }
    }
    return this;
  }

  public StatMap<K> merge(DataInput input)
      throws IOException {
    byte serializedKeys = input.readByte();

    K[] keys = (K[]) KEYS_BY_CLASS.computeIfAbsent(_keyClass, k -> k.getEnumConstants());
    for (byte i = 0; i < serializedKeys; i++) {
      int ordinal = input.readByte();
      K key = keys[ordinal];
      switch (key.getType()) {
        case BOOLEAN:
          merge(key, true);
          break;
        case INT:
          merge(key, input.readInt());
          break;
        case LONG:
          merge(key, input.readLong());
          break;
        case STRING:
          merge(key, input.readUTF());
          break;
        default:
          throw new IllegalStateException("Unknown type " + key.getType());
      }
    }
    return this;
  }

  public ObjectNode asJson() {
    ObjectNode node = JsonUtils.newObjectNode();

    for (Map.Entry<K, Object> entry : _map.entrySet()) {
      K key = entry.getKey();
      Object value = entry.getValue();
      switch (key.getType()) {
        case BOOLEAN:
          if (value == null) {
            if (key.includeDefaultInJson()) {
              node.put(key.getStatName(), false);
            }
          } else {
            node.put(key.getStatName(), (boolean) value);
          }
          break;
        case INT:
          if (value == null) {
            if (key.includeDefaultInJson()) {
              node.put(key.getStatName(), 0);
            }
          } else {
            node.put(key.getStatName(), (int) value);
          }
          break;
        case LONG:
          if (value == null) {
            if (key.includeDefaultInJson()) {
              node.put(key.getStatName(), 0L);
            }
          } else {
            node.put(key.getStatName(), (long) value);
          }
          break;
        case STRING:
          if (value == null) {
            if (key.includeDefaultInJson()) {
              node.put(key.getStatName(), "");
            }
          } else {
            node.put(key.getStatName(), (String) value);
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + key.getType());
      }
    }

    return node;
  }

  public void serialize(DataOutput output)
      throws IOException {

    assert checkContainsNoDefault() : "No default value should be stored in the map";
    output.writeByte(_map.size());

    // We use written keys just to fail fast in tests if the number of keys written
    // is not the same as the number of keys
    int writtenKeys = 0;
    K[] keys = (K[]) KEYS_BY_CLASS.computeIfAbsent(_keyClass, k -> k.getEnumConstants());
    for (int ordinal = 0; ordinal < keys.length; ordinal++) {
      K key = keys[ordinal];
      switch (key.getType()) {
        case BOOLEAN: {
          if (getBoolean(key)) {
            writtenKeys++;
            output.writeByte(ordinal);
          }
          break;
        }
        case INT: {
          int value = getInt(key);
          if (value != 0) {
            writtenKeys++;
            output.writeByte(ordinal);
            output.writeInt(value);
          }
          break;
        }
        case LONG: {
          long value = getLong(key);
          if (value != 0) {
            writtenKeys++;
            output.writeByte(ordinal);
            output.writeLong(value);
          }
          break;
        }
        case STRING: {
          String value = getString(key);
          if (value != null) {
            writtenKeys++;
            output.writeByte(ordinal);
            output.writeUTF(value);
          }
          break;
        }
        default:
          throw new IllegalStateException("Unknown type " + key.getType());
      }
    }
    assert writtenKeys == _map.size() : "Written keys " + writtenKeys + " but map size " + _map.size();
  }

  private boolean checkContainsNoDefault() {
    for (Map.Entry<K, Object> entry : _map.entrySet()) {
      K key = entry.getKey();
      Object value = entry.getValue();
      switch (key.getType()) {
        case BOOLEAN:
          if (value == null || !(boolean) value) {
            throw new IllegalStateException("Boolean value must be true but " + value + " is stored for key " + key);
          }
          break;
        case INT:
          if (value == null || (int) value == 0) {
            throw new IllegalStateException("Int value must be non-zero but " + value + " is stored for key " + key);
          }
          break;
        case LONG:
          if (value == null || (long) value == 0) {
            throw new IllegalStateException("Long value must be non-zero but " + value + " is stored for key " + key);
          }
          break;
        case STRING:
          if (value == null) {
            throw new IllegalStateException("String value must be non-null but null is stored for key " + key);
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported type: " + key.getType());
      }
    }
    return true;
  }

  public static String getDefaultStatName(Key key) {
    String name = key.name();
    StringBuilder result = new StringBuilder();
    boolean capitalizeNext = false;

    for (char c : name.toCharArray()) {
      if (c == '_') {
        capitalizeNext = true;
      } else {
        if (capitalizeNext) {
          result.append(c);
          capitalizeNext = false;
        } else {
          result.append(Character.toLowerCase(c));
        }
      }
    }

    return result.toString();
  }

  public static <K extends Enum<K> & Key> StatMap<K> deserialize(DataInput input, Class<K> keyClass)
      throws IOException {
    StatMap<K> result = new StatMap<>(keyClass);
    result.merge(input);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StatMap<?> statMap = (StatMap<?>) o;
    return Objects.equals(_map, statMap._map);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_map);
  }

  @Override
  public String toString() {
    return asJson().toString();
  }

  public Class<K> getKeyClass() {
    return _keyClass;
  }

  public boolean isEmpty() {
    return _map.isEmpty();
  }

  public Iterable<K> keySet() {
    return _map.keySet();
  }

  public interface Key {
    String name();

    /**
     * The name of the stat used to report it. Names must be unique on the same key family.
     */
    default String getStatName() {
      return getDefaultStatName(this);
    }

    default int merge(int value1, int value2) {
      return value1 + value2;
    }

    default long merge(long value1, long value2) {
      return value1 + value2;
    }

    default boolean merge(boolean value1, boolean value2) {
      return value1 || value2;
    }

    default String merge(@Nullable String value1, @Nullable String value2) {
      return value2 != null ? value2 : value1;
    }

    /**
     * The type of the values associated to this key.
     */
    Type getType();

    default boolean includeDefaultInJson() {
      return false;
    }

    static int minPositive(int value1, int value2) {
      if (value1 == 0 && value2 >= 0) {
        return value2;
      }
      if (value2 == 0 && value1 >= 0) {
        return value1;
      }
      return Math.min(value1, value2);
    }

    static long minPositive(long value1, long value2) {
      if (value1 == 0 && value2 >= 0) {
        return value2;
      }
      if (value2 == 0 && value1 >= 0) {
        return value1;
      }
      return Math.min(value1, value2);
    }

    static int eqNotZero(int value1, int value2) {
      if (value1 != value2) {
        if (value1 == 0) {
          return value2;
        } else if (value2 == 0) {
          return value1;
        } else {
          throw new IllegalStateException("Cannot merge non-zero values: " + value1 + " and " + value2);
        }
      }
      return value1;
    }
  }

  public enum Type {
    BOOLEAN,
    INT,
    LONG,
    STRING
  }
}
