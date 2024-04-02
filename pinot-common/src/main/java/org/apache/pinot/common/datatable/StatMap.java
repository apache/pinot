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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * A map that stores statistics.
 *
 * Statistics must be keyed by an enum that implements {@link StatMap.Key}.
 *
 * A stat map efficiently store, serialize and deserialize these statistics.
 *
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
  private final Family<K> _family;
  @Nullable
  private final int[] _intValues;
  @Nullable
  private final long[] _longValues;
  @Nullable
  private final boolean[] _booleanValues;
  /**
   * In Pinot 1.1.0 this can only store String values, but it is prepared to be able to store any kind of object in
   * the future.
   */
  @Nullable
  private final Object[] _referenceValues;

  public StatMap(Class<K> keyClass) {
    Family<K> family = new Family<>(keyClass);
    _family = family;
    _intValues = family.createIntValues();
    _longValues = family.createLongValues();
    _booleanValues = family.createBooleanValues();
    _referenceValues = family.createReferenceValues();
  }

  private StatMap(Family<K> family, @Nullable int[] intValues, @Nullable long[] longValues,
      @Nullable boolean[] booleanValues, @Nullable Object[] referenceValues) {
    _family = family;
    assert intValues == null || intValues.length == family._numIntsValues;
    _intValues = intValues;
    assert longValues == null || longValues.length == family._numLongValues;
    _longValues = longValues;
    assert booleanValues == null || booleanValues.length == family._numBooleanValues;
    _booleanValues = booleanValues;
    assert referenceValues == null || referenceValues.length == family._numReferenceValues;
    _referenceValues = referenceValues;
  }

  public int getInt(K key) {
    Preconditions.checkArgument(key.getType() == Type.INT);
    int index = _family.getIndex(key);
    assert _intValues != null : "Int values should not be null because " + key + " is of type INT";
    return _intValues[index];
  }

  public void merge(K key, int value) {
    if (key.getType() == Type.LONG) {
      merge(key, (long) value);
      return;
    }
    Preconditions.checkArgument(key.getType() == Type.INT);
    int index = _family.getIndex(key);
    assert _intValues != null : "Int values should not be null because " + key + " is of type INT";
    _intValues[index] = key.merge(_intValues[index], value);
  }

  public long getLong(K key) {
    if (key.getType() == Type.INT) {
      return getInt(key);
    }
    Preconditions.checkArgument(key.getType() == Type.LONG);
    int index = _family.getIndex(key);
    assert _longValues != null : "Long values should not be null because " + key + " is of type LONG";
    return _longValues[index];
  }

  public void merge(K key, long value) {
    Preconditions.checkArgument(key.getType() == Type.LONG);
    int index = _family.getIndex(key);
    assert _longValues != null : "Long values should not be null because " + key + " is of type LONG";
    _longValues[index] = key.merge(_longValues[index], value);
  }

  public boolean getBoolean(K key) {
    Preconditions.checkArgument(key.getType() == Type.BOOLEAN);
    int index = _family.getIndex(key);
    assert _booleanValues != null : "Boolean values should not be null because " + key + " is of type BOOLEAN";
    return _booleanValues[index];
  }

  public void merge(K key, boolean value) {
    Preconditions.checkArgument(key.getType() == Type.BOOLEAN);
    int index = _family.getIndex(key);
    assert _booleanValues != null : "Boolean values should not be null because " + key + " is of type BOOLEAN";
    _booleanValues[index] = key.merge(_booleanValues[index], value);
  }

  public String getString(K key) {
    Preconditions.checkArgument(key.getType() == Type.STRING);
    int index = _family.getIndex(key);
    assert _referenceValues != null : "Reference values should not be null because " + key + " is of type STRING";
    return (String) _referenceValues[index];
  }

  public void merge(K key, String value) {
    Preconditions.checkArgument(key.getType() == Type.STRING);
    int index = _family.getIndex(key);
    assert _referenceValues != null : "Reference values should not be null because " + key + " is of type STRING";
    _referenceValues[index] = key.merge((String) _referenceValues[index], value);
  }

  /**
   * Returns the value associated with the key.
   *
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
   * Numbers will be added, booleans will be ORed, and strings will be set if they are null.
   *
   * @param other The object to merge with. This argument will not be modified.
   */
  public void merge(StatMap<K> other) {
    Preconditions.checkState(_family._keyClass.equals(other._family._keyClass),
        "Different key classes %s and %s", _family._keyClass, other._family._keyClass);
    Preconditions.checkState(_family._numIntsValues == other._family._numIntsValues,
        "Different number of int values");
    for (int i = 0; i < _family._numIntsValues; i++) {
      assert _intValues != null : "Int values should not be null because there are int keys";
      assert other._intValues != null : "Int values should not be null because there are int keys";
      K key = _family.getKey(i, Type.INT);
      _intValues[i] = key.merge(_intValues[i], other._intValues[i]);
    }

    Preconditions.checkState(_family._numLongValues == other._family._numLongValues,
        "Different number of long values");
    for (int i = 0; i < _family._numLongValues; i++) {
      assert _longValues != null : "Long values should not be null because there are long keys";
      assert other._longValues != null : "Long values should not be null because there are long keys";
      K key = _family.getKey(i, Type.LONG);
      _longValues[i] = key.merge(_longValues[i], other._longValues[i]);
    }

    Preconditions.checkState(_family._numBooleanValues == other._family._numBooleanValues,
        "Different number of boolean values");
    for (int i = 0; i < _family._numBooleanValues; i++) {
      assert _booleanValues != null : "Boolean values should not be null because there are boolean keys";
      assert other._booleanValues != null : "Boolean values should not be null because there are boolean keys";
      K key = _family.getKey(i, Type.BOOLEAN);
      _booleanValues[i] = key.merge(_booleanValues[i], other._booleanValues[i]);
    }

    Preconditions.checkState(_family._numReferenceValues == other._family._numReferenceValues,
        "Different number of reference values");
    for (int i = 0; i < _family._numReferenceValues; i++) {
      assert _referenceValues != null : "Reference values should not be null because there are reference keys";
      assert other._referenceValues != null : "Reference values should not be null because there are reference keys";
      if (_referenceValues[i] == null) {
        _referenceValues[i] = other._referenceValues[i];
      } else {
        K key = _family.getKey(i, Type.STRING);
        _referenceValues[i] = key.merge((String) _referenceValues[i], (String) other._referenceValues[i]);
      }
    }
  }

  public void merge(DataInput input)
      throws IOException {
    int bitsPerId = 32 - Integer.numberOfLeadingZeros(Math.abs(_family._maxIndex));
    int maxBytesPerId = (bitsPerId + 7) / 8;

    int bytesPerId = input.readInt();
    Preconditions.checkArgument(bytesPerId <= maxBytesPerId, "Invalid bytes per id: %s. Max expected = %s",
        bytesPerId, maxBytesPerId);

    int[] keys;
    keys = readKeys(input, bytesPerId, _family._numIntsValues);
    if (_family._numIntsValues != 0) {
      assert _intValues != null : "Int values should not be null because there are int keys";
      for (int intKey : keys) {
        int value = input.readInt();
        assert value != 0;
        K key = _family.getKey(intKey, Type.INT);
        _intValues[intKey] = key.merge(_intValues[intKey], value);
      }
    }

    keys = readKeys(input, bytesPerId, _family._numLongValues);
    if (_family._numLongValues != 0) {
      assert _longValues != null : "Long values should not be null because there are long keys";
      for (int longKey : keys) {
        long value = input.readLong();
        assert value != 0;
        K key = _family.getKey(longKey, Type.LONG);
        _longValues[longKey] = key.merge(_longValues[longKey], value);
      }
    }

    keys = readKeys(input, bytesPerId, _family._numBooleanValues);
    if (_family._numBooleanValues != 0) {
      assert _booleanValues != null : "Boolean values should not be null because there are boolean keys";
      for (int booleanKey : keys) {
        boolean value = input.readBoolean();
        assert value;
        K key = _family.getKey(booleanKey, Type.BOOLEAN);
        _booleanValues[booleanKey] = key.merge(_booleanValues[booleanKey], true);
      }
    }

    keys = readKeys(input, bytesPerId, _family._numReferenceValues);
    if (_family._numReferenceValues != 0) {
      assert _referenceValues != null : "Reference values should not be null because there are reference keys";
      for (int refKeys : keys) {
        // In case we add more reference keys, this should be changed
        String value = input.readUTF();
        K key = _family.getKey(refKeys, Type.STRING);
        _referenceValues[refKeys] = key.merge((String) _referenceValues[refKeys], value);
      }
    }
  }

  public ObjectNode asJson() {
    ObjectNode node = JsonUtils.newObjectNode();

    if (_intValues != null) {
      for (K key : _family.getKeysOfType(Type.INT)) {
        int index = _family.getIndex(key);
        int value = _intValues[index];
        if (value != 0 || key.includeDefaultInJson()) {
          node.put(key.getStatName(), value);
        }
      }
    }

    if (_longValues != null) {
      for (K key : _family.getKeysOfType(Type.LONG)) {
        int index = _family.getIndex(key);
        long value = _longValues[index];
        if (value != 0 || key.includeDefaultInJson()) {
          node.put(key.getStatName(), value);
        }
      }
    }

    if (_booleanValues != null) {
      for (K key : _family.getKeysOfType(Type.BOOLEAN)) {
        int index = _family.getIndex(key);
        boolean value = _booleanValues[index];
        if (value || key.includeDefaultInJson()) {
          node.put(key.getStatName(), true);
        }
      }
    }

    if (_referenceValues != null) {
      for (K key : _family.getKeysOfType(Type.STRING)) {
        int index = _family.getIndex(key);
        String value = (String) _referenceValues[index];
        if (value != null || key.includeDefaultInJson()) {
          node.put(key.getStatName(), value);
        }
      }
    }

    return node;
  }

  public void serialize(DataOutput output)
      throws IOException {
    int maxIndex = _family._maxIndex;
    int bitsPerId = 32 - Integer.numberOfLeadingZeros(Math.abs(maxIndex));
    int bytesPerId = (bitsPerId + 7) / 8;

    output.writeInt(bytesPerId);

    writeInts(output, bytesPerId);
    writeLongs(output, bytesPerId);
    writeBooleans(output, bytesPerId);
    writeStrings(output, bytesPerId);
  }

  private void writeInts(DataOutput output, int bytesPerId)
      throws IOException {
    if (_intValues == null) {
      writeInt(output, bytesPerId, 0);
    } else {
      writeValuedKeys(output, Type.INT, bytesPerId, _intValues.length, i -> _intValues[i] != 0);
      for (int intValue : _intValues) {
        if (intValue != 0) {
          output.writeInt(intValue);
        }
      }
    }
  }

  private void writeLongs(DataOutput output, int bytesPerId)
      throws IOException {
    if (_longValues == null) {
      writeInt(output, bytesPerId, 0);
    } else {
      writeValuedKeys(output, Type.LONG, bytesPerId, _longValues.length, i -> _longValues[i] != 0);
      for (long longValue : _longValues) {
        if (longValue != 0) {
          output.writeLong(longValue);
        }
      }
    }
  }

  private void writeBooleans(DataOutput output, int bytesPerId)
      throws IOException {
    if (_booleanValues == null) {
      writeInt(output, bytesPerId, 0);
    } else {
      writeValuedKeys(output, Type.BOOLEAN, bytesPerId, _booleanValues.length, i -> _booleanValues[i]);
      for (boolean booleanValue : _booleanValues) {
        if (booleanValue) {
          output.writeBoolean(true);
        }
      }
    }
  }

  private void writeStrings(DataOutput output, int bytesPerId)
      throws IOException {
    if (_referenceValues == null) {
      writeInt(output, bytesPerId, 0);
    } else {
      writeValuedKeys(output, Type.STRING, bytesPerId, _referenceValues.length, i -> _referenceValues[i] != null);
      for (Object referenceValue : _referenceValues) {
        if (referenceValue != null) {
          output.writeUTF((String) referenceValue);
        }
      }
    }
  }

  private void writeValuedKeys(DataOutput output, Type type, int bytesPerId, int length,
      IntPredicate isValidIndex)
      throws IOException {
    List<K> allKeys = _family.getKeysOfType(type);
    List<K> keys = new ArrayList<>(allKeys.size());
    for (int i = 0; i < length; i++) {
      if (isValidIndex.test(i)) {
        keys.add(allKeys.get(i));
      }
    }
    writeInt(output, bytesPerId, keys.size());
    for (K key : keys) {
      writeInt(output, bytesPerId, _family.getIndex(key));
    }
  }

  public static <K extends Enum<K> & Key> StatMap<K> deserialize(DataInput input, Class<K> keyClass)
      throws IOException {
    Family<K> family = new Family<>(keyClass);

    int bytesPerId = input.readInt();

    int[] intKeys = readKeys(input, bytesPerId, family._numIntsValues);
    int[] intValues;
    if (family._numIntsValues == 0) {
      intValues = null;
    } else {
      intValues = new int[family._numIntsValues];
      for (int intKey : intKeys) {
        int value = input.readInt();
        assert value != 0;
        intValues[intKey] = value;
      }
    }

    int[] longKeys = readKeys(input, bytesPerId, family._numLongValues);
    long[] longValues;
    if (family._numLongValues == 0) {
      longValues = null;
    } else {
      longValues = new long[family._numLongValues];
      for (int longKey : longKeys) {
        long value = input.readLong();
        assert value != 0;
        longValues[longKey] = value;
      }
    }

    int[] booleanKeys = readKeys(input, bytesPerId, family._numBooleanValues);
    boolean[] booleanValues;
    if (family._numBooleanValues == 0) {
      booleanValues = null;
    } else {
      booleanValues = new boolean[family._numBooleanValues];
      for (int booleanKey : booleanKeys) {
        boolean value = input.readBoolean();
        assert value;
        booleanValues[booleanKey] = true;
      }
    }

    int[] stringKeys = readKeys(input, bytesPerId, family._numReferenceValues);
    Object[] referenceValues;
    if (family._numReferenceValues == 0) {
      referenceValues = null;
    } else {
      referenceValues = new Object[family._numReferenceValues];
      for (int stringKey : stringKeys) {
        String value = input.readUTF();
        referenceValues[stringKey] = value;
      }
    }

    return new StatMap<>(family, intValues, longValues, booleanValues, referenceValues);
  }

  private static int[] readKeys(DataInput input, int bytesPerId, int maxExpectedKeys)
      throws IOException {
    // sender may have more or less keys than expected.
    // In case it has less, receiver extra keys will not be populated
    // In case it has more, sender extra keys will be ignored
    int numKeys = Math.min(readInt(input, bytesPerId), maxExpectedKeys);
    int[] keys;
    keys = new int[numKeys];
    for (int i = 0; i < numKeys; i++) {
      keys[i] = readInt(input, bytesPerId);
    }
    return keys;
  }

  private static void writeInt(DataOutput dos, int bytesPerId, int number) throws IOException {
    // Write the lowest bytes of the number into the DataOutputStream
    for (int i = 0; i < bytesPerId; i++) {
      byte b = (byte) (number & 0xFF); // Get the lowest byte of the number
      dos.writeByte(b); // Write the byte into the DataOutputStream
      number >>= 8; // Shift the number 8 bits to the right to get the next byte
    }
  }

  private static int readInt(DataInput dis, int bytesPerInt) throws IOException {
    // Read the lowest bytes from the DataInputStream and reconstruct the integer
    int number = 0;
    for (int i = 0; i < bytesPerInt; i++) {
      byte b = dis.readByte(); // Read the next byte from the DataInputStream
      number |= (b & 0xFF) << (8 * i); // Combine the byte into the integer
    }
    return number;
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
    return Objects.equals(_family, statMap._family) && Arrays.equals(_intValues, statMap._intValues)
        && Arrays.equals(_longValues, statMap._longValues) && Arrays.equals(_booleanValues, statMap._booleanValues)
        && Arrays.equals(_referenceValues, statMap._referenceValues);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(_family);
    result = 31 * result + Arrays.hashCode(_intValues);
    result = 31 * result + Arrays.hashCode(_longValues);
    result = 31 * result + Arrays.hashCode(_booleanValues);
    result = 31 * result + Arrays.hashCode(_referenceValues);
    return result;
  }

  @Override
  public String toString() {
    return asJson().toString();
  }

  public Class<K> getKeyClass() {
    return _family._keyClass;
  }

  public boolean isEmpty() {
    if (_intValues != null) {
      for (int intValue : _intValues) {
        if (intValue != 0) {
          return false;
        }
      }
    }
    if (_longValues != null) {
      for (long longValue : _longValues) {
        if (longValue != 0) {
          return false;
        }
      }
    }
    if (_booleanValues != null) {
      for (boolean booleanValue : _booleanValues) {
        if (booleanValue) {
          return false;
        }
      }
    }
    if (_referenceValues != null) {
      for (Object referenceValue : _referenceValues) {
        if (referenceValue != null) {
          return false;
        }
      }
    }
    return true;
  }

  private static class Family<K extends Enum<K> & Key> {
    private final Class<K> _keyClass;
    private final int _numIntsValues;
    private final int _numLongValues;
    private final int _numBooleanValues;
    private final int _numReferenceValues;
    private final int _maxIndex;
    private final EnumMap<K, Integer> _keyToIndexMap;

    private Family(Class<K> keyClass) {
      _keyClass = keyClass;
      K[] keys = keyClass.getEnumConstants();
      int numIntsValues = 0;
      int numLongValues = 0;
      int numBooleanValues = 0;
      int numReferenceValues = 0;
      _keyToIndexMap = new EnumMap<>(keyClass);
      for (K key : keys) {
        switch (key.getType()) {
          case BOOLEAN:
            _keyToIndexMap.put(key, numBooleanValues++);
            break;
          case INT:
            _keyToIndexMap.put(key, numIntsValues++);
            break;
          case LONG:
            _keyToIndexMap.put(key, numLongValues++);
            break;
          case STRING:
            _keyToIndexMap.put(key, numReferenceValues++);
            break;
          default:
            throw new IllegalStateException();
        }
      }
      _numIntsValues = numIntsValues;
      _numLongValues = numLongValues;
      _numBooleanValues = numBooleanValues;
      _numReferenceValues = numReferenceValues;
      _maxIndex = Math.max(numIntsValues, Math.max(numLongValues, Math.max(numBooleanValues, numReferenceValues)));
    }

    @Nullable
    private int[] createIntValues() {
      if (_numIntsValues == 0) {
        return null;
      }
      return new int[_numIntsValues];
    }

    @Nullable
    private long[] createLongValues() {
      if (_numLongValues == 0) {
        return null;
      }
      return new long[_numLongValues];
    }

    @Nullable
    private boolean[] createBooleanValues() {
      if (_numBooleanValues == 0) {
        return null;
      }
      return new boolean[_numBooleanValues];
    }

    @Nullable
    private Object[] createReferenceValues() {
      if (_numReferenceValues == 0) {
        return null;
      }
      return new Object[_numReferenceValues];
    }

    private int getIndex(K key) {
      Integer i = _keyToIndexMap.get(key);
      if (i == null) {
        throw new IllegalArgumentException("Unknown key: " + key + " for family " + _keyClass);
      }
      return i;
    }

    private K getKey(int index, Type type) {
      for (Map.Entry<K, Integer> entry : _keyToIndexMap.entrySet()) {
        if (entry.getValue() == index && entry.getKey().getType() == type) {
          return entry.getKey();
        }
      }
      throw new IllegalArgumentException("Unknown key for index " + index + " and type " + type);
    }

    /**
     * Returns a list with the keys of a given type.
     *
     * The iteration order is the ascending order of calling {@code #getIndex(K)}.
     */
    private List<K> getKeysOfType(Type type) {
      return Arrays.stream(_keyClass.getEnumConstants())
          .filter(key -> key.getType() == type)
          .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Family<?> family = (Family<?>) o;
      return Objects.equals(_keyClass, family._keyClass);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_keyClass);
    }

    @Override
    public String toString() {
      return "Family{" + "_keyClass=" + _keyClass + '}';
    }
  }

  public interface Key {
    String name();

    /**
     * The name of the stat used to report it. Names must be unique on the same key family.
     */
    default String getStatName() {
      String name = name();
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

    default int merge(int value1, int value2) {
      return value1 + value2;
    }

    default long merge(long value1, long value2) {
      return value1 + value2;
    }

    default double merge(double value1, double value2) {
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
  }

  public enum Type {
    BOOLEAN,
    INT,
    LONG,
    STRING
  }
}
