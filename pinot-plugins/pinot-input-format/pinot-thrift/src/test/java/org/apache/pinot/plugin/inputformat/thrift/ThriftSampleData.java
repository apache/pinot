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
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.pinot.plugin.inputformat.thrift;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.transport.TIOStreamTransport;


public class ThriftSampleData implements TBase<ThriftSampleData, ThriftSampleData._Fields>, Serializable, Cloneable,
    Comparable<ThriftSampleData> {
  private static final TStruct STRUCT_DESC = new TStruct("ThriftSampleData");
  private static final TField ID_FIELD_DESC = new TField("id", (byte) 8, (short) 1);
  private static final TField NAME_FIELD_DESC = new TField("name", (byte) 11, (short) 2);
  private static final TField CREATED_AT_FIELD_DESC = new TField("created_at", (byte) 10, (short) 3);
  private static final TField ACTIVE_FIELD_DESC = new TField("active", (byte) 2, (short) 4);
  private static final TField GROUPS_FIELD_DESC = new TField("groups", (byte) 15, (short) 5);
  private static final TField MAP_VALUES_FIELD_DESC = new TField("map_values", (byte) 13, (short) 6);
  private static final TField SET_VALUES_FIELD_DESC = new TField("set_values", (byte) 14, (short) 7);
  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap();
  public int id;
  public String name;
  public long created_at;
  public boolean active;
  public List<Short> groups;
  public Map<String, Long> map_values;
  public Set<String> set_values;
  private static final int __ID_ISSET_ID = 0;
  private static final int __CREATED_AT_ISSET_ID = 1;
  private static final int __ACTIVE_ISSET_ID = 2;
  private byte __isset_bitfield;
  private static final ThriftSampleData._Fields[] optionals;
  public static final Map<ThriftSampleData._Fields, FieldMetaData> metaDataMap;

  public ThriftSampleData() {
    this.__isset_bitfield = 0;
  }

  public ThriftSampleData(List<Short> groups, Map<String, Long> map_values, Set<String> set_values) {
    this();
    this.groups = groups;
    this.map_values = map_values;
    this.set_values = set_values;
  }

  public ThriftSampleData(ThriftSampleData other) {
    this.__isset_bitfield = 0;
    this.__isset_bitfield = other.__isset_bitfield;
    this.id = other.id;
    if (other.isSetName()) {
      this.name = other.name;
    }

    this.created_at = other.created_at;
    this.active = other.active;
    if (other.isSetGroups()) {
      List<Short> __this__groups = new ArrayList(other.groups);
      this.groups = __this__groups;
    }

    if (other.isSetMap_values()) {
      Map<String, Long> __this__map_values = new HashMap(other.map_values);
      this.map_values = __this__map_values;
    }

    if (other.isSetSet_values()) {
      Set<String> __this__set_values = new HashSet(other.set_values);
      this.set_values = __this__set_values;
    }
  }

  public ThriftSampleData deepCopy() {
    return new ThriftSampleData(this);
  }

  public void clear() {
    this.setIdIsSet(false);
    this.id = 0;
    this.name = null;
    this.setCreated_atIsSet(false);
    this.created_at = 0L;
    this.setActiveIsSet(false);
    this.active = false;
    this.groups = null;
    this.map_values = null;
    this.set_values = null;
  }

  public int getId() {
    return this.id;
  }

  public ThriftSampleData setId(int id) {
    this.id = id;
    this.setIdIsSet(true);
    return this;
  }

  public void unsetId() {
    this.__isset_bitfield = EncodingUtils.clearBit(this.__isset_bitfield, 0);
  }

  public boolean isSetId() {
    return EncodingUtils.testBit(this.__isset_bitfield, 0);
  }

  public void setIdIsSet(boolean value) {
    this.__isset_bitfield = EncodingUtils.setBit(this.__isset_bitfield, 0, value);
  }

  public String getName() {
    return this.name;
  }

  public ThriftSampleData setName(String name) {
    this.name = name;
    return this;
  }

  public void unsetName() {
    this.name = null;
  }

  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  public long getCreated_at() {
    return this.created_at;
  }

  public ThriftSampleData setCreated_at(long created_at) {
    this.created_at = created_at;
    this.setCreated_atIsSet(true);
    return this;
  }

  public void unsetCreated_at() {
    this.__isset_bitfield = EncodingUtils.clearBit(this.__isset_bitfield, 1);
  }

  public boolean isSetCreated_at() {
    return EncodingUtils.testBit(this.__isset_bitfield, 1);
  }

  public void setCreated_atIsSet(boolean value) {
    this.__isset_bitfield = EncodingUtils.setBit(this.__isset_bitfield, 1, value);
  }

  public boolean isActive() {
    return this.active;
  }

  public ThriftSampleData setActive(boolean active) {
    this.active = active;
    this.setActiveIsSet(true);
    return this;
  }

  public void unsetActive() {
    this.__isset_bitfield = EncodingUtils.clearBit(this.__isset_bitfield, 2);
  }

  public boolean isSetActive() {
    return EncodingUtils.testBit(this.__isset_bitfield, 2);
  }

  public void setActiveIsSet(boolean value) {
    this.__isset_bitfield = EncodingUtils.setBit(this.__isset_bitfield, 2, value);
  }

  public int getGroupsSize() {
    return this.groups == null ? 0 : this.groups.size();
  }

  public Iterator<Short> getGroupsIterator() {
    return this.groups == null ? null : this.groups.iterator();
  }

  public void addToGroups(short elem) {
    if (this.groups == null) {
      this.groups = new ArrayList();
    }

    this.groups.add(elem);
  }

  public List<Short> getGroups() {
    return this.groups;
  }

  public ThriftSampleData setGroups(List<Short> groups) {
    this.groups = groups;
    return this;
  }

  public void unsetGroups() {
    this.groups = null;
  }

  public boolean isSetGroups() {
    return this.groups != null;
  }

  public void setGroupsIsSet(boolean value) {
    if (!value) {
      this.groups = null;
    }
  }

  public int getMap_valuesSize() {
    return this.map_values == null ? 0 : this.map_values.size();
  }

  public void putToMap_values(String key, long val) {
    if (this.map_values == null) {
      this.map_values = new HashMap();
    }

    this.map_values.put(key, val);
  }

  public Map<String, Long> getMap_values() {
    return this.map_values;
  }

  public ThriftSampleData setMap_values(Map<String, Long> map_values) {
    this.map_values = map_values;
    return this;
  }

  public void unsetMap_values() {
    this.map_values = null;
  }

  public boolean isSetMap_values() {
    return this.map_values != null;
  }

  public void setMap_valuesIsSet(boolean value) {
    if (!value) {
      this.map_values = null;
    }
  }

  public int getSet_valuesSize() {
    return this.set_values == null ? 0 : this.set_values.size();
  }

  public Iterator<String> getSet_valuesIterator() {
    return this.set_values == null ? null : this.set_values.iterator();
  }

  public void addToSet_values(String elem) {
    if (this.set_values == null) {
      this.set_values = new HashSet();
    }

    this.set_values.add(elem);
  }

  public Set<String> getSet_values() {
    return this.set_values;
  }

  public ThriftSampleData setSet_values(Set<String> set_values) {
    this.set_values = set_values;
    return this;
  }

  public void unsetSet_values() {
    this.set_values = null;
  }

  public boolean isSetSet_values() {
    return this.set_values != null;
  }

  public void setSet_valuesIsSet(boolean value) {
    if (!value) {
      this.set_values = null;
    }
  }

  public void setFieldValue(ThriftSampleData._Fields field, Object value) {
    switch (field) {
      case ID:
        if (value == null) {
          this.unsetId();
        } else {
          this.setId(((Integer) value).intValue());
        }
        break;
      case NAME:
        if (value == null) {
          this.unsetName();
        } else {
          this.setName((String) value);
        }
        break;
      case CREATED_AT:
        if (value == null) {
          this.unsetCreated_at();
        } else {
          this.setCreated_at(((Long) value).longValue());
        }
        break;
      case ACTIVE:
        if (value == null) {
          this.unsetActive();
        } else {
          this.setActive(((Boolean) value).booleanValue());
        }
        break;
      case GROUPS:
        if (value == null) {
          this.unsetGroups();
        } else {
          this.setGroups((List) value);
        }
        break;
      case MAP_VALUES:
        if (value == null) {
          this.unsetMap_values();
        } else {
          this.setMap_values((Map) value);
        }
        break;
      case SET_VALUES:
        if (value == null) {
          this.unsetSet_values();
        } else {
          this.setSet_values((Set) value);
        }
    }
  }

  public Object getFieldValue(ThriftSampleData._Fields field) {
    switch (field) {
      case ID:
        return this.getId();
      case NAME:
        return this.getName();
      case CREATED_AT:
        return this.getCreated_at();
      case ACTIVE:
        return this.isActive();
      case GROUPS:
        return this.getGroups();
      case MAP_VALUES:
        return this.getMap_values();
      case SET_VALUES:
        return this.getSet_values();
      default:
        throw new IllegalStateException();
    }
  }

  public boolean isSet(ThriftSampleData._Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    } else {
      switch (field) {
        case ID:
          return this.isSetId();
        case NAME:
          return this.isSetName();
        case CREATED_AT:
          return this.isSetCreated_at();
        case ACTIVE:
          return this.isSetActive();
        case GROUPS:
          return this.isSetGroups();
        case MAP_VALUES:
          return this.isSetMap_values();
        case SET_VALUES:
          return this.isSetSet_values();
        default:
          throw new IllegalStateException();
      }
    }
  }

  public boolean equals(Object that) {
    if (that == null) {
      return false;
    } else {
      return that instanceof ThriftSampleData ? this.equals((ThriftSampleData) that) : false;
    }
  }

  public boolean equals(ThriftSampleData that) {
    if (that == null) {
      return false;
    } else {
      boolean this_present_id = this.isSetId();
      boolean that_present_id = that.isSetId();
      if (this_present_id || that_present_id) {
        if (!this_present_id || !that_present_id) {
          return false;
        }

        if (this.id != that.id) {
          return false;
        }
      }

      boolean this_present_name = this.isSetName();
      boolean that_present_name = that.isSetName();
      if (this_present_name || that_present_name) {
        if (!this_present_name || !that_present_name) {
          return false;
        }

        if (!this.name.equals(that.name)) {
          return false;
        }
      }

      boolean this_present_created_at = this.isSetCreated_at();
      boolean that_present_created_at = that.isSetCreated_at();
      if (this_present_created_at || that_present_created_at) {
        if (!this_present_created_at || !that_present_created_at) {
          return false;
        }

        if (this.created_at != that.created_at) {
          return false;
        }
      }

      boolean this_present_active = this.isSetActive();
      boolean that_present_active = that.isSetActive();
      if (this_present_active || that_present_active) {
        if (!this_present_active || !that_present_active) {
          return false;
        }

        if (this.active != that.active) {
          return false;
        }
      }

      boolean this_present_groups = this.isSetGroups();
      boolean that_present_groups = that.isSetGroups();
      if (this_present_groups || that_present_groups) {
        if (!this_present_groups || !that_present_groups) {
          return false;
        }

        if (!this.groups.equals(that.groups)) {
          return false;
        }
      }

      boolean this_present_map_values = this.isSetMap_values();
      boolean that_present_map_values = that.isSetMap_values();
      if (this_present_map_values || that_present_map_values) {
        if (!this_present_map_values || !that_present_map_values) {
          return false;
        }

        if (!this.map_values.equals(that.map_values)) {
          return false;
        }
      }

      boolean this_present_set_values = this.isSetSet_values();
      boolean that_present_set_values = that.isSetSet_values();
      if (this_present_set_values || that_present_set_values) {
        if (!this_present_set_values || !that_present_set_values) {
          return false;
        }

        if (!this.set_values.equals(that.set_values)) {
          return false;
        }
      }

      return true;
    }
  }

  public int hashCode() {
    List<Object> list = new ArrayList();
    boolean present_id = this.isSetId();
    list.add(present_id);
    if (present_id) {
      list.add(this.id);
    }

    boolean present_name = this.isSetName();
    list.add(present_name);
    if (present_name) {
      list.add(this.name);
    }

    boolean present_created_at = this.isSetCreated_at();
    list.add(present_created_at);
    if (present_created_at) {
      list.add(this.created_at);
    }

    boolean present_active = this.isSetActive();
    list.add(present_active);
    if (present_active) {
      list.add(this.active);
    }

    boolean present_groups = this.isSetGroups();
    list.add(present_groups);
    if (present_groups) {
      list.add(this.groups);
    }

    boolean present_map_values = this.isSetMap_values();
    list.add(present_map_values);
    if (present_map_values) {
      list.add(this.map_values);
    }

    boolean present_set_values = this.isSetSet_values();
    list.add(present_set_values);
    if (present_set_values) {
      list.add(this.set_values);
    }

    return list.hashCode();
  }

  public int compareTo(ThriftSampleData other) {
    if (!this.getClass().equals(other.getClass())) {
      return this.getClass().getName().compareTo(other.getClass().getName());
    } else {
      int lastComparison = 0;
      lastComparison = Boolean.valueOf(this.isSetId()).compareTo(other.isSetId());
      if (lastComparison != 0) {
        return lastComparison;
      } else {
        if (this.isSetId()) {
          lastComparison = TBaseHelper.compareTo(this.id, other.id);
          if (lastComparison != 0) {
            return lastComparison;
          }
        }

        lastComparison = Boolean.valueOf(this.isSetName()).compareTo(other.isSetName());
        if (lastComparison != 0) {
          return lastComparison;
        } else {
          if (this.isSetName()) {
            lastComparison = TBaseHelper.compareTo(this.name, other.name);
            if (lastComparison != 0) {
              return lastComparison;
            }
          }

          lastComparison = Boolean.valueOf(this.isSetCreated_at()).compareTo(other.isSetCreated_at());
          if (lastComparison != 0) {
            return lastComparison;
          } else {
            if (this.isSetCreated_at()) {
              lastComparison = TBaseHelper.compareTo(this.created_at, other.created_at);
              if (lastComparison != 0) {
                return lastComparison;
              }
            }

            lastComparison = Boolean.valueOf(this.isSetActive()).compareTo(other.isSetActive());
            if (lastComparison != 0) {
              return lastComparison;
            } else {
              if (this.isSetActive()) {
                lastComparison = TBaseHelper.compareTo(this.active, other.active);
                if (lastComparison != 0) {
                  return lastComparison;
                }
              }

              lastComparison = Boolean.valueOf(this.isSetGroups()).compareTo(other.isSetGroups());
              if (lastComparison != 0) {
                return lastComparison;
              } else {
                if (this.isSetGroups()) {
                  lastComparison = TBaseHelper.compareTo(this.groups, other.groups);
                  if (lastComparison != 0) {
                    return lastComparison;
                  }
                }

                lastComparison = Boolean.valueOf(this.isSetMap_values()).compareTo(other.isSetMap_values());
                if (lastComparison != 0) {
                  return lastComparison;
                } else {
                  if (this.isSetMap_values()) {
                    lastComparison = TBaseHelper.compareTo(this.map_values, other.map_values);
                    if (lastComparison != 0) {
                      return lastComparison;
                    }
                  }

                  lastComparison = Boolean.valueOf(this.isSetSet_values()).compareTo(other.isSetSet_values());
                  if (lastComparison != 0) {
                    return lastComparison;
                  } else {
                    if (this.isSetSet_values()) {
                      lastComparison = TBaseHelper.compareTo(this.set_values, other.set_values);
                      if (lastComparison != 0) {
                        return lastComparison;
                      }
                    }

                    return 0;
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public ThriftSampleData._Fields fieldForId(int fieldId) {
    return ThriftSampleData._Fields.findByThriftId(fieldId);
  }

  public void read(TProtocol iprot) throws TException {
    ((SchemeFactory) schemes.get(iprot.getScheme())).getScheme().read(iprot, this);
  }

  public void write(TProtocol oprot) throws TException {
    ((SchemeFactory) schemes.get(oprot.getScheme())).getScheme().write(oprot, this);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("ThriftSampleData(");
    boolean first = true;
    if (this.isSetId()) {
      sb.append("id:");
      sb.append(this.id);
      first = false;
    }

    if (this.isSetName()) {
      if (!first) {
        sb.append(", ");
      }

      sb.append("name:");
      if (this.name == null) {
        sb.append("null");
      } else {
        sb.append(this.name);
      }

      first = false;
    }

    if (this.isSetCreated_at()) {
      if (!first) {
        sb.append(", ");
      }

      sb.append("created_at:");
      sb.append(this.created_at);
      first = false;
    }

    if (this.isSetActive()) {
      if (!first) {
        sb.append(", ");
      }

      sb.append("active:");
      sb.append(this.active);
      first = false;
    }

    if (!first) {
      sb.append(", ");
    }

    sb.append("groups:");
    if (this.groups == null) {
      sb.append("null");
    } else {
      sb.append(this.groups);
    }

    first = false;
    if (!first) {
      sb.append(", ");
    }

    sb.append("map_values:");
    if (this.map_values == null) {
      sb.append("null");
    } else {
      sb.append(this.map_values);
    }

    first = false;
    if (!first) {
      sb.append(", ");
    }

    sb.append("set_values:");
    if (this.set_values == null) {
      sb.append("null");
    } else {
      sb.append(this.set_values);
    }

    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    try {
      this.write(new TCompactProtocol(new TIOStreamTransport(out)));
    } catch (TException var3) {
      throw new IOException(var3);
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    try {
      this.__isset_bitfield = 0;
      this.read(new TCompactProtocol(new TIOStreamTransport(in)));
    } catch (TException var3) {
      throw new IOException(var3);
    }
  }

  static {
    schemes.put(StandardScheme.class, new ThriftSampleData.ThriftSampleData1StandardSchemeFactory());
    schemes.put(TupleScheme.class, new ThriftSampleData.ThriftSampleData1TupleSchemeFactory());
    optionals =
        new ThriftSampleData._Fields[]{ThriftSampleData._Fields.ID, ThriftSampleData._Fields.NAME, ThriftSampleData._Fields.CREATED_AT, ThriftSampleData._Fields.ACTIVE};
    Map<ThriftSampleData._Fields, FieldMetaData> tmpMap = new EnumMap(ThriftSampleData._Fields.class);
    tmpMap.put(ThriftSampleData._Fields.ID, new FieldMetaData("id", (byte) 2, new FieldValueMetaData((byte) 8)));
    tmpMap.put(ThriftSampleData._Fields.NAME, new FieldMetaData("name", (byte) 2, new FieldValueMetaData((byte) 11)));
    tmpMap.put(ThriftSampleData._Fields.CREATED_AT,
        new FieldMetaData("created_at", (byte) 2, new FieldValueMetaData((byte) 10)));
    tmpMap.put(ThriftSampleData._Fields.ACTIVE,
        new FieldMetaData("active", (byte) 2, new FieldValueMetaData((byte) 2)));
    tmpMap.put(ThriftSampleData._Fields.GROUPS,
        new FieldMetaData("groups", (byte) 3, new ListMetaData((byte) 15, new FieldValueMetaData((byte) 6))));
    tmpMap.put(ThriftSampleData._Fields.MAP_VALUES, new FieldMetaData("map_values", (byte) 3,
        new MapMetaData((byte) 13, new FieldValueMetaData((byte) 11), new FieldValueMetaData((byte) 10))));
    tmpMap.put(ThriftSampleData._Fields.SET_VALUES,
        new FieldMetaData("set_values", (byte) 3, new SetMetaData((byte) 14, new FieldValueMetaData((byte) 11))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(ThriftSampleData.class, metaDataMap);
  }

  private static class ThriftSampleData1TupleScheme extends TupleScheme<ThriftSampleData> {
    private ThriftSampleData1TupleScheme() {
    }

    public void write(TProtocol prot, ThriftSampleData struct) throws TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetId()) {
        optionals.set(0);
      }

      if (struct.isSetName()) {
        optionals.set(1);
      }

      if (struct.isSetCreated_at()) {
        optionals.set(2);
      }

      if (struct.isSetActive()) {
        optionals.set(3);
      }

      if (struct.isSetGroups()) {
        optionals.set(4);
      }

      if (struct.isSetMap_values()) {
        optionals.set(5);
      }

      if (struct.isSetSet_values()) {
        optionals.set(6);
      }

      oprot.writeBitSet(optionals, 7);
      if (struct.isSetId()) {
        oprot.writeI32(struct.id);
      }

      if (struct.isSetName()) {
        oprot.writeString(struct.name);
      }

      if (struct.isSetCreated_at()) {
        oprot.writeI64(struct.created_at);
      }

      if (struct.isSetActive()) {
        oprot.writeBool(struct.active);
      }

      Iterator var5;
      if (struct.isSetGroups()) {
        oprot.writeI32(struct.groups.size());
        var5 = struct.groups.iterator();

        while (var5.hasNext()) {
          short _iter13 = ((Short) var5.next()).shortValue();
          oprot.writeI16(_iter13);
        }
      }

      if (struct.isSetMap_values()) {
        oprot.writeI32(struct.map_values.size());
        var5 = struct.map_values.entrySet().iterator();

        while (var5.hasNext()) {
          Entry<String, Long> _iter14 = (Entry) var5.next();
          oprot.writeString((String) _iter14.getKey());
          oprot.writeI64(((Long) _iter14.getValue()).longValue());
        }
      }

      if (struct.isSetSet_values()) {
        oprot.writeI32(struct.set_values.size());
        var5 = struct.set_values.iterator();

        while (var5.hasNext()) {
          String _iter15 = (String) var5.next();
          oprot.writeString(_iter15);
        }
      }
    }

    public void read(TProtocol prot, ThriftSampleData struct) throws TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.id = iprot.readI32();
        struct.setIdIsSet(true);
      }

      if (incoming.get(1)) {
        struct.name = iprot.readString();
        struct.setNameIsSet(true);
      }

      if (incoming.get(2)) {
        struct.created_at = iprot.readI64();
        struct.setCreated_atIsSet(true);
      }

      if (incoming.get(3)) {
        struct.active = iprot.readBool();
        struct.setActiveIsSet(true);
      }

      int _i25;
      if (incoming.get(4)) {
        TList _list16 = new TList((byte) 6, iprot.readI32());
        struct.groups = new ArrayList(_list16.size);

        for (_i25 = 0; _i25 < _list16.size; ++_i25) {
          short _elem17 = iprot.readI16();
          struct.groups.add(_elem17);
        }

        struct.setGroupsIsSet(true);
      }

      String _elem24;
      if (incoming.get(5)) {
        TMap _map19 = new TMap((byte) 11, (byte) 10, iprot.readI32());
        struct.map_values = new HashMap(2 * _map19.size);

        for (int _i22 = 0; _i22 < _map19.size; ++_i22) {
          _elem24 = iprot.readString();
          long _val21 = iprot.readI64();
          struct.map_values.put(_elem24, _val21);
        }

        struct.setMap_valuesIsSet(true);
      }

      if (incoming.get(6)) {
        TSet _set23 = new TSet((byte) 11, iprot.readI32());
        struct.set_values = new HashSet(2 * _set23.size);

        for (_i25 = 0; _i25 < _set23.size; ++_i25) {
          _elem24 = iprot.readString();
          struct.set_values.add(_elem24);
        }

        struct.setSet_valuesIsSet(true);
      }
    }
  }

  private static class ThriftSampleData1TupleSchemeFactory implements SchemeFactory {
    private ThriftSampleData1TupleSchemeFactory() {
    }

    public ThriftSampleData.ThriftSampleData1TupleScheme getScheme() {
      return new ThriftSampleData.ThriftSampleData1TupleScheme();
    }
  }

  private static class ThriftSampleData1StandardScheme extends StandardScheme<ThriftSampleData> {
    private ThriftSampleData1StandardScheme() {
    }

    public void read(TProtocol iprot, ThriftSampleData struct) throws TException {
      iprot.readStructBegin();

      while (true) {
        TField schemeField = iprot.readFieldBegin();
        if (schemeField.type == 0) {
          iprot.readStructEnd();
          struct.validate();
          return;
        }

        String _elem8;
        int _i9;
        switch (schemeField.id) {
          case 1:
            if (schemeField.type == 8) {
              struct.id = iprot.readI32();
              struct.setIdIsSet(true);
            } else {
              TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2:
            if (schemeField.type == 11) {
              struct.name = iprot.readString();
              struct.setNameIsSet(true);
            } else {
              TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3:
            if (schemeField.type == 10) {
              struct.created_at = iprot.readI64();
              struct.setCreated_atIsSet(true);
            } else {
              TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4:
            if (schemeField.type == 2) {
              struct.active = iprot.readBool();
              struct.setActiveIsSet(true);
            } else {
              TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5:
            if (schemeField.type != 15) {
              TProtocolUtil.skip(iprot, schemeField.type);
              break;
            }

            TList _list0 = iprot.readListBegin();
            struct.groups = new ArrayList(_list0.size);

            for (_i9 = 0; _i9 < _list0.size; ++_i9) {
              short _elem1 = iprot.readI16();
              struct.groups.add(_elem1);
            }

            iprot.readListEnd();
            struct.setGroupsIsSet(true);
            break;
          case 6:
            if (schemeField.type != 13) {
              TProtocolUtil.skip(iprot, schemeField.type);
              break;
            }

            TMap _map3 = iprot.readMapBegin();
            struct.map_values = new HashMap(2 * _map3.size);

            for (int _i6 = 0; _i6 < _map3.size; ++_i6) {
              _elem8 = iprot.readString();
              long _val5 = iprot.readI64();
              struct.map_values.put(_elem8, _val5);
            }

            iprot.readMapEnd();
            struct.setMap_valuesIsSet(true);
            break;
          case 7:
            if (schemeField.type != 14) {
              TProtocolUtil.skip(iprot, schemeField.type);
              break;
            }

            TSet _set7 = iprot.readSetBegin();
            struct.set_values = new HashSet(2 * _set7.size);

            for (_i9 = 0; _i9 < _set7.size; ++_i9) {
              _elem8 = iprot.readString();
              struct.set_values.add(_elem8);
            }

            iprot.readSetEnd();
            struct.setSet_valuesIsSet(true);
            break;
          default:
            TProtocolUtil.skip(iprot, schemeField.type);
        }

        iprot.readFieldEnd();
      }
    }

    public void write(TProtocol oprot, ThriftSampleData struct) throws TException {
      struct.validate();
      oprot.writeStructBegin(ThriftSampleData.STRUCT_DESC);
      if (struct.isSetId()) {
        oprot.writeFieldBegin(ThriftSampleData.ID_FIELD_DESC);
        oprot.writeI32(struct.id);
        oprot.writeFieldEnd();
      }

      if (struct.name != null && struct.isSetName()) {
        oprot.writeFieldBegin(ThriftSampleData.NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }

      if (struct.isSetCreated_at()) {
        oprot.writeFieldBegin(ThriftSampleData.CREATED_AT_FIELD_DESC);
        oprot.writeI64(struct.created_at);
        oprot.writeFieldEnd();
      }

      if (struct.isSetActive()) {
        oprot.writeFieldBegin(ThriftSampleData.ACTIVE_FIELD_DESC);
        oprot.writeBool(struct.active);
        oprot.writeFieldEnd();
      }

      Iterator var3;
      if (struct.groups != null) {
        oprot.writeFieldBegin(ThriftSampleData.GROUPS_FIELD_DESC);
        oprot.writeListBegin(new TList((byte) 6, struct.groups.size()));
        var3 = struct.groups.iterator();

        while (var3.hasNext()) {
          short _iter10 = ((Short) var3.next()).shortValue();
          oprot.writeI16(_iter10);
        }

        oprot.writeListEnd();
        oprot.writeFieldEnd();
      }

      if (struct.map_values != null) {
        oprot.writeFieldBegin(ThriftSampleData.MAP_VALUES_FIELD_DESC);
        oprot.writeMapBegin(new TMap((byte) 11, (byte) 10, struct.map_values.size()));
        var3 = struct.map_values.entrySet().iterator();

        while (var3.hasNext()) {
          Entry<String, Long> _iter11 = (Entry) var3.next();
          oprot.writeString((String) _iter11.getKey());
          oprot.writeI64(((Long) _iter11.getValue()).longValue());
        }

        oprot.writeMapEnd();
        oprot.writeFieldEnd();
      }

      if (struct.set_values != null) {
        oprot.writeFieldBegin(ThriftSampleData.SET_VALUES_FIELD_DESC);
        oprot.writeSetBegin(new TSet((byte) 11, struct.set_values.size()));
        var3 = struct.set_values.iterator();

        while (var3.hasNext()) {
          String _iter12 = (String) var3.next();
          oprot.writeString(_iter12);
        }

        oprot.writeSetEnd();
        oprot.writeFieldEnd();
      }

      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
  }

  private static class ThriftSampleData1StandardSchemeFactory implements SchemeFactory {
    private ThriftSampleData1StandardSchemeFactory() {
    }

    public ThriftSampleData.ThriftSampleData1StandardScheme getScheme() {
      return new ThriftSampleData.ThriftSampleData1StandardScheme();
    }
  }

  public static enum _Fields implements TFieldIdEnum {
    ID((short) 1, "id"),
    NAME((short) 2, "name"),
    CREATED_AT((short) 3, "created_at"),
    ACTIVE((short) 4, "active"),
    GROUPS((short) 5, "groups"),
    MAP_VALUES((short) 6, "map_values"),
    SET_VALUES((short) 7, "set_values");

    private static final Map<String, ThriftSampleData._Fields> byName = new HashMap();
    private final short _thriftId;
    private final String _fieldName;

    public static ThriftSampleData._Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        case 1:
          return ID;
        case 2:
          return NAME;
        case 3:
          return CREATED_AT;
        case 4:
          return ACTIVE;
        case 5:
          return GROUPS;
        case 6:
          return MAP_VALUES;
        case 7:
          return SET_VALUES;
        default:
          return null;
      }
    }

    public static ThriftSampleData._Fields findByThriftIdOrThrow(int fieldId) {
      ThriftSampleData._Fields fields = findByThriftId(fieldId);
      if (fields == null) {
        throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      } else {
        return fields;
      }
    }

    public static ThriftSampleData._Fields findByName(String name) {
      return (ThriftSampleData._Fields) byName.get(name);
    }

    private _Fields(short thriftId, String fieldName) {
      this._thriftId = thriftId;
      this._fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return this._thriftId;
    }

    public String getFieldName() {
      return this._fieldName;
    }

    static {
      Iterator var0 = EnumSet.allOf(ThriftSampleData._Fields.class).iterator();

      while (var0.hasNext()) {
        ThriftSampleData._Fields field = (ThriftSampleData._Fields) var0.next();
        byName.put(field.getFieldName(), field);
      }
    }
  }
}
