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
package com.linkedin.pinot.pql.parsers.utils;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.FieldInfo;
import com.alibaba.fastjson.util.TypeUtils;


public class JSONUtil {
  public static class FastJSONObject extends JSONObject {
    private com.alibaba.fastjson.JSONObject _inner;

    public FastJSONObject() {
      _inner = new com.alibaba.fastjson.JSONObject();
    }

    public FastJSONObject(String str) throws JSONException {
      try {
        _inner = (com.alibaba.fastjson.JSONObject) JSON.parse(str);
      } catch (Exception e) {
        throw new JSONException(e);
      }
    }

    public FastJSONObject(Map value) {
      if (value instanceof com.alibaba.fastjson.JSONObject) {
        _inner = (com.alibaba.fastjson.JSONObject) value;
      } else {
        _inner = (com.alibaba.fastjson.JSONObject) toJSON(value);
      }
    }

    public FastJSONObject(com.alibaba.fastjson.JSONObject inner) {
      _inner = inner;
    }

    public com.alibaba.fastjson.JSONObject getInnerJSONObject() {
      return _inner;
    }

    /**
     * Accumulate values under a key. It is similar to the put method except
     * that if there is already an object stored under the key then a
     * JSONArray is stored under the key to hold all of the accumulated values.
     * If there is already a JSONArray, then the new value is appended to it.
     * In contrast, the put method replaces the previous value.
     *
     * If only one value is accumulated that is not a JSONArray, then the
     * result will be the same as using put. But if multiple values are
     * accumulated, then the result will be like append.
     * @param key   A key string.
     * @param value An object to be accumulated under the key.
     * @return this.
     * @throws JSONException If the value is an invalid number
     *  or if the key is null.
     */
    @Override
    public JSONObject accumulate(String key, Object value) throws JSONException {
      Object object = _inner.get(key);
      if (object == null) {
        com.alibaba.fastjson.JSONArray array = new com.alibaba.fastjson.JSONArray();
        array.add(value);
        _inner.put(key, array);
      } else if (object instanceof com.alibaba.fastjson.JSONArray) {
        ((com.alibaba.fastjson.JSONArray) object).add(value);
      } else {
        com.alibaba.fastjson.JSONArray array = new com.alibaba.fastjson.JSONArray();
        array.add(object);
        array.add(value);
        _inner.put(key, array);
      }
      return this;
    }

    /**
     * Append values to the array under a key. If the key does not exist in the
     * JSONObject, then the key is put in the JSONObject with its value being a
     * JSONArray containing the value parameter. If the key was already
     * associated with a JSONArray, then the value parameter is appended to it.
     * @param key   A key string.
     * @param value An object to be accumulated under the key.
     * @return this.
     * @throws JSONException If the key is null or if the current value
     *  associated with the key is not a JSONArray.
     */
    @Override
    public JSONObject append(String key, Object value) throws JSONException {
      Object object = _inner.get(key);
      if (object == null) {
        com.alibaba.fastjson.JSONArray array = new com.alibaba.fastjson.JSONArray();
        array.add(value);
        _inner.put(key, array);
      } else if (object instanceof com.alibaba.fastjson.JSONArray) {
        ((com.alibaba.fastjson.JSONArray) object).add(value);
      } else {
        throw new JSONException("JSONObject[" + key + "] is not a JSONArray.");
      }
      return this;
    }

    /**
     * Get the value object associated with a key.
     *
     * @param key   A key string.
     * @return    The object associated with the key.
     * @throws    JSONException if the key is not found.
     */
    @Override
    public Object get(String key) throws JSONException {
      Object object = this.opt(key);
      if (object == null) {
        throw new JSONException("JSONObject[" + key + "] not found.");
      }
      return object;
    }

    /**
     * Get the JSONArray value associated with a key.
     *
     * @param key   A key string.
     * @return    A JSONArray which is the value.
     * @throws    JSONException if the key is not found or
     *  if the value is not a JSONArray.
     */
    @Override
    public JSONArray getJSONArray(String key) throws JSONException {
      Object object = this.get(key);
      if (object instanceof com.alibaba.fastjson.JSONArray) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) object);
      } else if (object instanceof Collection) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) toJSON(object));
      } else if (object instanceof JSONArray) {
        return (JSONArray) object;
      }
      throw new JSONException("JSONObject[" + quote(key) + "] is not a JSONArray.");
    }

    /**
     * Get the JSONObject value associated with a key.
     *
     * @param key   A key string.
     * @return    A JSONObject which is the value.
     * @throws    JSONException if the key is not found or
     *  if the value is not a JSONObject.
     */
    @Override
    public JSONObject getJSONObject(String key) throws JSONException {
      Object object = this.get(key);
      if (object instanceof com.alibaba.fastjson.JSONObject) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) object);
      } else if (object instanceof Map) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) toJSON(object));
      } else if (object instanceof JSONObject) {
        return (JSONObject) object;
      }
      throw new JSONException("JSONObject[" + quote(key) + "] is not a JSONObject.");
    }

    /**
     * Determine if the JSONObject contains a specific key.
     * @param key   A key string.
     * @return    true if the key exists in the JSONObject.
     */
    @Override
    public boolean has(String key) {
      return _inner.containsKey(key);
    }

    /**
     * Get an enumeration of the keys of the JSONObject.
     *
     * @return An iterator of the keys.
     */
    @Override
    public Iterator keys() {
      return _inner.keySet().iterator();
    }

    @Override
    public Iterator sortedKeys() {
      return new TreeSet(_inner.keySet()).iterator();
    }

    /**
     * Get the number of keys stored in the JSONObject.
     *
     * @return The number of keys in the JSONObject.
     */
    @Override
    public int length() {
      return _inner.size();
    }

    /**
     * Produce a JSONArray containing the names of the elements of this
     * JSONObject.
     * @return A JSONArray containing the key strings, or null if the JSONObject
     * is empty.
     */
    @Override
    public JSONArray names() {
      JSONArray ja = new FastJSONArray();
      Iterator keys = this.keys();
      while (keys.hasNext()) {
        ja.put(keys.next());
      }
      return ja.length() == 0 ? null : ja;
    }

    /**
     * Get an optional value associated with a key.
     * @param key   A key string.
     * @return    An object which is the value, or null if there is no value.
     */
    @Override
    public Object opt(String key) {
      if (key == null) {
        return null;
      }

      Object object = _inner.get(key);
      if (object == null) {
        return null;
      } else if (object instanceof com.alibaba.fastjson.JSONObject) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) object);
      } else if (object instanceof com.alibaba.fastjson.JSONArray) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) object);
      } else if (object instanceof Map) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) toJSON(object));
      } else if (object instanceof Collection) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) toJSON(object));
      }
      return object;
    }

    /**
     * Get an optional JSONArray associated with a key.
     * It returns null if there is no such key, or if its value is not a
     * JSONArray.
     *
     * @param key   A key string.
     * @return    A JSONArray which is the value.
     */
    @Override
    public JSONArray optJSONArray(String key) {
      try {
        return this.getJSONArray(key);
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * Get an optional JSONObject associated with a key.
     * It returns null if there is no such key, or if its value is not a
     * JSONObject.
     *
     * @param key   A key string.
     * @return    A JSONObject which is the value.
     */
    @Override
    public JSONObject optJSONObject(String key) {
      try {
        return this.getJSONObject(key);
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * Get an optional string associated with a key.
     * It returns the defaultValue if there is no such key.
     *
     * @param key   A key string.
     * @param defaultValue   The default.
     * @return    A string which is the value.
     */
    @Override
    public String optString(String key, String defaultValue) {
      Object object = this.opt(key);
      if (object == null || NULL.equals(object)) {
        return defaultValue;
      }
      return object.toString();
    }

    /**
     * Put a key/value pair in the JSONObject, where the value will be a
     * JSONArray which is produced from a Collection.
     * @param key   A key string.
     * @param value A Collection value.
     * @return    this.
     * @throws JSONException
     */
    @Override
    public JSONObject put(String key, Collection value) throws JSONException {
      this.put(key, (Object) value);
      return this;
    }

    /**
     * Put a key/value pair in the JSONObject, where the value will be a
     * JSONObject which is produced from a Map.
     * @param key   A key string.
     * @param value A Map value.
     * @return    this.
     * @throws JSONException
     */
    @Override
    public JSONObject put(String key, Map value) throws JSONException {
      this.put(key, (Object) value);
      return this;
    }

    /**
     * Put a key/value pair in the JSONObject. If the value is null,
     * then the key will be removed from the JSONObject if it is present.
     * @param key   A key string.
     * @param value An object which is the value. It should be of one of these
     *  types: Boolean, Double, Integer, JSONArray, JSONObject, Long, String,
     *  or the JSONObject.NULL object.
     * @return this.
     * @throws JSONException If the value is non-finite number
     *  or if the key is null.
     */
    @Override
    public JSONObject put(String key, Object value) throws JSONException {
      if (key == null) {
        throw new JSONException("Null key.");
      }
      if (value != null) {
        if (value instanceof FastJSONObject) {
          _inner.put(key, ((FastJSONObject) value).getInnerJSONObject());
        } else if (value instanceof FastJSONArray) {
          _inner.put(key, ((FastJSONArray) value).getInnerJSONArray());
        } else {
          if (value instanceof JSONObject || value instanceof JSONArray) {
            throw new JSONException("the value is not fast version of JSONObjects: " + value);
          }
          _inner.put(key, toJSON(value));
        }
      } else {
        this.remove(key);
      }
      return this;
    }

    /**
     * Remove a name and its value, if present.
     * @param key The name to be removed.
     * @return The value that was associated with the name,
     * or null if there was no value.
     */
    @Override
    public Object remove(String key) {
      return _inner.remove(key);
    }

    /**
     * Make a JSON text of this JSONObject. For compactness, no whitespace
     * is added. If this would not result in a syntactically correct JSON text,
     * then null will be returned instead.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return a printable, displayable, portable, transmittable
     *  representation of the object, beginning
     *  with <code>{</code>&nbsp;<small>(left brace)</small> and ending
     *  with <code>}</code>&nbsp;<small>(right brace)</small>.
     */
    @Override
    public String toString() {
      try {
        return _inner.toString();
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * Not a prettyprinted JSON text, just the same as toString().
     */
    @Override
    public String toString(int indentFactor) throws JSONException {
      return _inner.toString();
    }
  }

  public static class FastJSONArray extends JSONArray {
    private com.alibaba.fastjson.JSONArray _inner;

    /**
     * Construct an empty JSONArray.
     */
    public FastJSONArray() {
      _inner = new com.alibaba.fastjson.JSONArray();
    }

    public FastJSONArray(String str) throws JSONException {
      try {
        _inner = (com.alibaba.fastjson.JSONArray) JSON.parse(str);
      } catch (Exception e) {
        throw new JSONException(e);
      }
    }

    public FastJSONArray(Collection value) {
      if (value instanceof com.alibaba.fastjson.JSONArray) {
        _inner = (com.alibaba.fastjson.JSONArray) value;
      } else {
        _inner = (com.alibaba.fastjson.JSONArray) toJSON(value);
      }
    }

    public FastJSONArray(com.alibaba.fastjson.JSONArray inner) {
      _inner = inner;
    }

    public com.alibaba.fastjson.JSONArray getInnerJSONArray() {
      return _inner;
    }

    /**
     * Get the object value associated with an index.
     * @param index
     *  The index must be between 0 and length() - 1.
     * @return An object value.
     * @throws JSONException If there is no value for the index.
     */
    @Override
    public Object get(int index) throws JSONException {
      Object object = this.opt(index);
      if (object == null) {
        throw new JSONException("JSONArray[" + index + "] not found.");
      }
      return object;
    }

    /**
     * Get the JSONArray associated with an index.
     * @param index The index must be between 0 and length() - 1.
     * @return    A JSONArray value.
     * @throws JSONException If there is no value for the index. or if the
     * value is not a JSONArray
     */
    @Override
    public JSONArray getJSONArray(int index) throws JSONException {
      Object object = this.get(index);
      if (object instanceof com.alibaba.fastjson.JSONArray) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) object);
      } else if (object instanceof Collection) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) toJSON(object));
      } else if (object instanceof JSONArray) {
        return (JSONArray) object;
      }
      throw new JSONException("JSONArray[" + index + "] is not a JSONArray.");
    }

    /**
     * Get the JSONObject associated with an index.
     * @param index subscript
     * @return    A JSONObject value.
     * @throws JSONException If there is no value for the index or if the
     * value is not a JSONObject
     */
    @Override
    public JSONObject getJSONObject(int index) throws JSONException {
      Object object = this.get(index);
      if (object instanceof com.alibaba.fastjson.JSONObject) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) object);
      } else if (object instanceof Map) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) toJSON(object));
      } else if (object instanceof JSONObject) {
        return (JSONObject) object;
      }
      throw new JSONException("JSONArray[" + index + "] is not a JSONObject.");
    }

    /**
     * Get the number of elements in the JSONArray, included nulls.
     *
     * @return The length (or size).
     */
    @Override
    public int length() {
      return _inner.size();
    }

    /**
     * Get the optional object value associated with an index.
     * @param index The index must be between 0 and length() - 1.
     * @return    An object value, or null if there is no
     *        object at that index.
     */
    @Override
    public Object opt(int index) {
      if (index < 0 || index >= this.length()) {
        return null;
      }
      Object object = _inner.get(index);
      if (object == null) {
        return null;
      } else if (object instanceof com.alibaba.fastjson.JSONObject) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) object);
      } else if (object instanceof com.alibaba.fastjson.JSONArray) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) object);
      } else if (object instanceof Map) {
        return new FastJSONObject((com.alibaba.fastjson.JSONObject) toJSON(object));
      } else if (object instanceof Collection) {
        return new FastJSONArray((com.alibaba.fastjson.JSONArray) toJSON(object));
      }
      return object;
    }

    /**
     * Get the optional JSONArray associated with an index.
     * @param index subscript
     * @return    A JSONArray value, or null if the index has no value,
     * or if the value is not a JSONArray.
     */
    @Override
    public JSONArray optJSONArray(int index) {
      try {
        return this.getJSONArray(index);
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * Get the optional JSONObject associated with an index.
     * Null is returned if the key is not found, or null if the index has
     * no value, or if the value is not a JSONObject.
     *
     * @param index The index must be between 0 and length() - 1.
     * @return    A JSONObject value.
     */
    @Override
    public JSONObject optJSONObject(int index) {
      try {
        return this.getJSONObject(index);
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * Get the optional string associated with an index.
     * The defaultValue is returned if the key is not found.
     *
     * @param index The index must be between 0 and length() - 1.
     * @param defaultValue   The default value.
     * @return    A String value.
     */
    @Override
    public String optString(int index, String defaultValue) {
      Object object = this.opt(index);
      if (object == null || JSONObject.NULL.equals(object)) {
        return defaultValue;
      }
      return object.toString();
    }

    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONArray which is produced from a Collection.
     * @param value A Collection value.
     * @return    this.
     */
    @Override
    public JSONArray put(Collection value) {
      this.put((Object) value);
      return this;
    }

    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONObject which is produced from a Map.
     * @param value A Map value.
     * @return    this.
     */
    @Override
    public JSONArray put(Map value) {
      this.put((Object) value);
      return this;
    }

    /**
     * Append an object value. This increases the array's length by one.
     * @param value An object value.  The value should be a
     *  Boolean, Double, Integer, JSONArray, JSONObject, Long, or String, or the
     *  JSONObject.NULL object.
     * @return this.
     */
    @Override
    public JSONArray put(Object value) {
      if (value instanceof FastJSONObject) {
        _inner.add(((FastJSONObject) value).getInnerJSONObject());
      } else if (value instanceof FastJSONArray) {
        _inner.add(((FastJSONArray) value).getInnerJSONArray());
      } else {
        if (value instanceof JSONObject || value instanceof JSONArray) {
          throw new IllegalArgumentException("the value is not fast version of JSONObjects: " + value);
        }
        _inner.add(toJSON(value));
      }
      return this;
    }

    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONArray which is produced from a Collection.
     * @param index The subscript.
     * @param value A Collection value.
     * @return    this.
     * @throws JSONException If the index is negative or if the value is
     * not finite.
     */
    @Override
    public JSONArray put(int index, Collection value) throws JSONException {
      this.put(index, (Object) value);
      return this;
    }

    /**
     * Put a value in the JSONArray, where the value will be a
     * JSONObject that is produced from a Map.
     * @param index The subscript.
     * @param value The Map value.
     * @return    this.
     * @throws JSONException If the index is negative or if the the value is
     *  an invalid number.
     */
    @Override
    public JSONArray put(int index, Map value) throws JSONException {
      this.put(index, (Object) value);
      return this;
    }

    /**
     * Put or replace an object value in the JSONArray. If the index is greater
     *  than the length of the JSONArray, then null elements will be added as
     *  necessary to pad it out.
     * @param index The subscript.
     * @param value The value to put into the array. The value should be a
     *  Boolean, Double, Integer, JSONArray, JSONObject, Long, or String, or the
     *  JSONObject.NULL object.
     * @return this.
     * @throws JSONException If the index is negative or if the the value is
     *  an invalid number.
     */
    @Override
    public JSONArray put(int index, Object value) throws JSONException {
      if (index < 0) {
        throw new JSONException("JSONArray[" + index + "] not found.");
      }
      if (index < this.length()) {
        if (value instanceof FastJSONObject) {
          _inner.set(index, ((FastJSONObject) value).getInnerJSONObject());
        } else if (value instanceof FastJSONArray) {
          _inner.set(index, ((FastJSONArray) value).getInnerJSONArray());
        } else {
          if (value instanceof JSONObject || value instanceof JSONArray) {
            throw new IllegalArgumentException("the value is not fast version of JSONObjects: " + value);
          }
          _inner.set(index, value);
        }
      } else {
        while (index != this.length()) {
          this.put(JSONObject.NULL);
        }
        this.put(value);
      }
      return this;
    }

    /**
     * Remove an index and close the hole.
     * @param index The index of the element to be removed.
     * @return The value that was associated with the index,
     * or null if there was no value.
     */
    public Object remove(int index) {
      Object o = this.opt(index);
      _inner.remove(index);
      return o;
    }

    /**
     * Make a JSON text of this JSONArray. For compactness, no
     * unnecessary whitespace is added. If it is not possible to produce a
     * syntactically correct JSON text then null will be returned instead. This
     * could occur if the array contains an invalid number.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return a printable, displayable, transmittable
     *  representation of the array.
     */
    @Override
    public String toString() {
      try {
        return _inner.toString();
      } catch (Exception e) {
        return null;
      }
    }

    /**
     * Not a prettyprinted JSON text, just the same as toString().
     */
    @Override
    public String toString(int indentFactor) throws JSONException {
      return this.toString();
    }
  }

  public static String normalize(String str) {
    return str.replaceAll("[\\s\\\\]*\n[\\s\\\\]*", " ");
  }

  @SuppressWarnings("unchecked")
  public static Object toJSON(Object javaObject) {
    if (javaObject == null) {
      return null;
    }

    if (javaObject instanceof JSON) {
      return javaObject;
    }

    if (javaObject instanceof FastJSONObject) {
      return ((FastJSONObject) javaObject).getInnerJSONObject();
    }

    if (javaObject instanceof FastJSONArray) {
      return ((FastJSONArray) javaObject).getInnerJSONArray();
    }

    if (javaObject instanceof Map) {
      Map<Object, Object> map = (Map<Object, Object>) javaObject;

      com.alibaba.fastjson.JSONObject json = new com.alibaba.fastjson.JSONObject(map.size());

      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        Object key = entry.getKey();
        String jsonKey = TypeUtils.castToString(key);
        Object jsonValue = toJSON(entry.getValue());
        json.put(jsonKey, jsonValue);
      }

      return json;
    }

    if (javaObject instanceof Collection) {
      Collection<Object> collection = (Collection<Object>) javaObject;

      com.alibaba.fastjson.JSONArray array = new com.alibaba.fastjson.JSONArray(collection.size());

      for (Object item : collection) {
        Object jsonValue = toJSON(item);
        array.add(jsonValue);
      }

      return array;
    }

    Class<?> clazz = javaObject.getClass();

    if (clazz.isEnum()) {
      return ((Enum<?>) javaObject).name();
    }

    if (clazz.isArray()) {
      int len = Array.getLength(javaObject);

      com.alibaba.fastjson.JSONArray array = new com.alibaba.fastjson.JSONArray(len);

      for (int i = 0; i < len; ++i) {
        Object item = Array.get(javaObject, i);
        Object jsonValue = toJSON(item);
        array.add(jsonValue);
      }

      return array;
    }

    if (ParserConfig.getGlobalInstance().isPrimitive(clazz)) {
      return javaObject;
    }

    try {
      List<FieldInfo> getters = TypeUtils.computeGetters(clazz, null);

      com.alibaba.fastjson.JSONObject json = new com.alibaba.fastjson.JSONObject(getters.size());

      for (FieldInfo field : getters) {
        Object value = field.get(javaObject);
        Object jsonValue = toJSON(value);

        json.put(field.getName(), jsonValue);
      }

      return json;
    } catch (Exception e) {
      throw new com.alibaba.fastjson.JSONException("toJSON error", e);
    }
  }

  public static boolean optBooleanValue(com.alibaba.fastjson.JSONObject json, String key)
      throws com.alibaba.fastjson.JSONException {
    return optBooleanValue(json, key, false);
  }

  public static boolean optBooleanValue(com.alibaba.fastjson.JSONObject json, String key, boolean defaultValue)
      throws com.alibaba.fastjson.JSONException {
    Boolean val = json.getBoolean(key);
    if (val == null)
      return defaultValue;
    return val.booleanValue();
  }

  public static boolean optBooleanValue(com.alibaba.fastjson.JSONArray json, int index)
      throws com.alibaba.fastjson.JSONException {
    return optBooleanValue(json, index, false);
  }

  public static boolean optBooleanValue(com.alibaba.fastjson.JSONArray json, int index, boolean defaultValue)
      throws com.alibaba.fastjson.JSONException {
    Boolean val = json.getBoolean(index);
    if (val == null)
      return defaultValue;
    return val.booleanValue();
  }

  public static com.alibaba.fastjson.JSONObject optJSONObject(com.alibaba.fastjson.JSONObject json, String key)
      throws com.alibaba.fastjson.JSONException {
    Object obj = json.get(key);
    if (obj == null)
      return null;

    if (!(obj instanceof com.alibaba.fastjson.JSONObject)) {
      try {
        return (com.alibaba.fastjson.JSONObject) toJSON(obj);
      } catch (Exception e) {
        return null;
      }
    }

    return (com.alibaba.fastjson.JSONObject) obj;
  }

  public static com.alibaba.fastjson.JSONObject optJSONObject(com.alibaba.fastjson.JSONArray json, int index)
      throws com.alibaba.fastjson.JSONException {
    Object obj = json.get(index);
    if (obj == null)
      return null;

    if (!(obj instanceof com.alibaba.fastjson.JSONObject)) {
      try {
        return (com.alibaba.fastjson.JSONObject) toJSON(obj);
      } catch (Exception e) {
        return null;
      }
    }

    return (com.alibaba.fastjson.JSONObject) obj;
  }

  public static com.alibaba.fastjson.JSONArray optJSONArray(com.alibaba.fastjson.JSONObject json, String key)
      throws com.alibaba.fastjson.JSONException {
    Object obj = json.get(key);
    if (obj == null)
      return null;

    if (!(obj instanceof com.alibaba.fastjson.JSONArray)) {
      try {
        return (com.alibaba.fastjson.JSONArray) toJSON(obj);
      } catch (Exception e) {
        return null;
      }
    }

    return (com.alibaba.fastjson.JSONArray) obj;
  }

  public static com.alibaba.fastjson.JSONArray optJSONArray(com.alibaba.fastjson.JSONArray json, int index)
      throws com.alibaba.fastjson.JSONException {
    Object obj = json.get(index);
    if (obj == null)
      return null;

    if (!(obj instanceof com.alibaba.fastjson.JSONArray)) {
      try {
        return (com.alibaba.fastjson.JSONArray) toJSON(obj);
      } catch (Exception e) {
        return null;
      }
    }

    return (com.alibaba.fastjson.JSONArray) obj;
  }
}
