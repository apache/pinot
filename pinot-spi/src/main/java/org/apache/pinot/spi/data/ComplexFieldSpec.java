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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;


/**
 * FieldSpec for complex fields. The {@link org.apache.pinot.spi.data.FieldSpec.FieldType}
 * is COMPLEX and the inner data type represents the root data type of the field.
 * It could be STRUCT, MAP, LIST or OPEN_STRUCT. A complex field is composable with a single root
 * type and a number of child types. Although we have multi-value primitive columns, LIST
 * is for representing lists of both complex and primitives inside a complex field.
 *
 * Consider a person json where the root type is STRUCT and composes of inner members:
 *  STRUCT(
 *          name: STRING
 *          age: INT
 *          salary: INT
 *          addresses: LIST (STRUCT
 *                              apt: INT
 *                              street: STRING
 *                              city: STRING
 *                              zip: INT
 *                          )
 *        )
 *
 * The fieldspec would be COMPLEX with type as STRUCT and 4 inner members
 * to model the hierarchy.
 *
 * For OPEN_STRUCT (semi-structured columns), per-key declared types are stored in
 * {@code valueFieldSpecs} and a fallback type for unlisted keys is stored in
 * {@code defaultValueFieldSpec}; {@code childFieldSpecs} is not used.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ComplexFieldSpec extends FieldSpec {
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  private final Map<String, FieldSpec> _childFieldSpecs;

  @Nullable
  private final Map<String, FieldSpec> _valueFieldSpecs;

  @Nullable
  private final FieldSpec _defaultValueFieldSpec;

  // Default constructor required by JSON de-serializer
  public ComplexFieldSpec() {
    this((Map<String, FieldSpec>) null, null);
  }

  @JsonCreator
  ComplexFieldSpec(@JsonProperty("valueFieldSpecs") @Nullable Map<String, FieldSpec> valueFieldSpecs,
      @JsonProperty("defaultValueFieldSpec") @Nullable FieldSpec defaultValueFieldSpec) {
    super();
    _childFieldSpecs = new HashMap<>();
    _valueFieldSpecs = valueFieldSpecs;
    _defaultValueFieldSpec = defaultValueFieldSpec;
  }

  public ComplexFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      Map<String, FieldSpec> childFieldSpecs) {
    this(name, dataType, isSingleValueField, childFieldSpecs, null, null);
  }

  public ComplexFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      Map<String, FieldSpec> childFieldSpecs, @Nullable Map<String, FieldSpec> valueFieldSpecs,
      @Nullable FieldSpec defaultValueFieldSpec) {
    super(name, dataType, isSingleValueField);
    Preconditions.checkArgument(
        dataType == DataType.STRUCT || dataType == DataType.MAP
            || dataType == DataType.LIST || dataType == DataType.OPEN_STRUCT,
        "ComplexFieldSpec dataType must be STRUCT, MAP, LIST, or OPEN_STRUCT (got %s)", dataType);
    if (dataType == DataType.MAP) {
      Preconditions.checkArgument(valueFieldSpecs == null,
          "DataType.MAP does not support valueFieldSpecs; use OPEN_STRUCT for per-key types");
      Preconditions.checkArgument(defaultValueFieldSpec == null,
          "DataType.MAP does not support defaultValueFieldSpec; use OPEN_STRUCT for a default type");
    }
    if (dataType == DataType.OPEN_STRUCT) {
      Preconditions.checkArgument(defaultValueFieldSpec != null,
          "DataType.OPEN_STRUCT requires defaultValueFieldSpec");
      Preconditions.checkArgument(childFieldSpecs == null || childFieldSpecs.isEmpty(),
          "DataType.OPEN_STRUCT does not use childFieldSpecs (KEY_FIELD/VALUE_FIELD)");
    }
    _childFieldSpecs = childFieldSpecs == null ? new HashMap<>() : new HashMap<>(childFieldSpecs);
    _valueFieldSpecs = valueFieldSpecs;
    _defaultValueFieldSpec = defaultValueFieldSpec;
  }

  public static String[] getColumnPath(String column) {
    return column.split("\\$\\$");
  }

  public FieldSpec getChildFieldSpec(String child) {
    return _childFieldSpecs.get(child);
  }

  public Map<String, FieldSpec> getChildFieldSpecs() {
    return _childFieldSpecs;
  }

  @JsonProperty("valueFieldSpecs")
  @Nullable
  public Map<String, FieldSpec> getValueFieldSpecs() {
    return _valueFieldSpecs;
  }

  @JsonProperty("defaultValueFieldSpec")
  @Nullable
  public FieldSpec getDefaultValueFieldSpec() {
    return _defaultValueFieldSpec;
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.COMPLEX;
  }

  @Override
  public String toString() {
    return "field type: COMPLEX, field name: " + _name + ", root data type: " + _dataType + ", child field specs: "
        + _childFieldSpecs;
  }

  public static class MapFieldSpec {
    private final String _fieldName;
    private final FieldSpec _keyFieldSpec;
    private final FieldSpec _valueFieldSpec;

    private MapFieldSpec(ComplexFieldSpec complexFieldSpec) {
      Preconditions.checkState(complexFieldSpec.getChildFieldSpecs().containsKey(KEY_FIELD),
          "Missing 'key' in the 'childFieldSpec'");
      Preconditions.checkState(complexFieldSpec.getChildFieldSpecs().containsKey(VALUE_FIELD),
          "Missing 'value' in the 'childFieldSpec'");
      _keyFieldSpec = complexFieldSpec.getChildFieldSpec(KEY_FIELD);
      _valueFieldSpec = complexFieldSpec.getChildFieldSpec(VALUE_FIELD);
      _fieldName = complexFieldSpec.getName();
    }

    public String getFieldName() {
      return _fieldName;
    }

    public FieldSpec getKeyFieldSpec() {
      return _keyFieldSpec;
    }

    public FieldSpec getValueFieldSpec() {
      return _valueFieldSpec;
    }
  }

  public static MapFieldSpec toMapFieldSpec(ComplexFieldSpec complexFieldSpec) {
    return new MapFieldSpec(complexFieldSpec);
  }

  public static ComplexFieldSpec fromMapFieldSpec(MapFieldSpec mapFieldSpec) {
    return new ComplexFieldSpec(mapFieldSpec.getFieldName(), DataType.MAP, true,
        Map.of(KEY_FIELD, mapFieldSpec.getKeyFieldSpec(), VALUE_FIELD, mapFieldSpec.getValueFieldSpec()));
  }

  /**
   * View over a {@link ComplexFieldSpec} whose {@code dataType} is {@link DataType#OPEN_STRUCT}.
   * Exposes the per-key {@code valueFieldSpecs} and the fallback {@code defaultValueFieldSpec}.
   */
  public static class OpenStructFieldSpec {
    private final String _fieldName;
    private final Map<String, FieldSpec> _valueFieldSpecs;
    private final FieldSpec _defaultValueFieldSpec;

    private OpenStructFieldSpec(ComplexFieldSpec complexFieldSpec) {
      Preconditions.checkArgument(complexFieldSpec.getDataType() == DataType.OPEN_STRUCT,
          "OpenStructFieldSpec view requires OPEN_STRUCT (got %s)", complexFieldSpec.getDataType());
      _fieldName = complexFieldSpec.getName();
      _valueFieldSpecs = complexFieldSpec.getValueFieldSpecs() != null
          ? complexFieldSpec.getValueFieldSpecs() : Map.of();
      _defaultValueFieldSpec = complexFieldSpec.getDefaultValueFieldSpec();
    }

    public String getFieldName() {
      return _fieldName;
    }

    public Map<String, FieldSpec> getValueFieldSpecs() {
      return _valueFieldSpecs;
    }

    public FieldSpec getDefaultValueFieldSpec() {
      return _defaultValueFieldSpec;
    }
  }

  public static OpenStructFieldSpec toOpenStructFieldSpec(ComplexFieldSpec complexFieldSpec) {
    return new OpenStructFieldSpec(complexFieldSpec);
  }

  public static ComplexFieldSpec fromOpenStructFieldSpec(OpenStructFieldSpec openStructFieldSpec) {
    return new ComplexFieldSpec(openStructFieldSpec.getFieldName(), DataType.OPEN_STRUCT, true,
        null, openStructFieldSpec.getValueFieldSpecs(),
        openStructFieldSpec.getDefaultValueFieldSpec());
  }

  /**
   * Returns the full child name for the given columns for complex data type.
   * E.g. map$$key, map$$value, list$$element, etc.
   * This is used in persisting column metadata for complex data types.
   */
  public static String getFullChildName(String... columns) {
    return StringUtil.join("$$", columns);
  }

  @Override
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = super.toJsonObject();
    if (_dataType == DataType.OPEN_STRUCT) {
      // Emit synthetic childFieldSpecs:{key,value} as a backward-compat shim so older readers
      // that don't know OPEN_STRUCT see a STRING/STRING MAP-shaped placeholder rather than
      // failing schema deserialization. New readers ignore childFieldSpecs when
      // dataType == OPEN_STRUCT and use valueFieldSpecs / defaultValueFieldSpec instead.
      ObjectNode childFieldSpecsNode = JsonUtils.newObjectNode();
      childFieldSpecsNode.set(KEY_FIELD,
          new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true).toJsonObject());
      childFieldSpecsNode.set(VALUE_FIELD,
          new DimensionFieldSpec(VALUE_FIELD, DataType.STRING, true).toJsonObject());
      jsonObject.set("childFieldSpecs", childFieldSpecsNode);

      if (_valueFieldSpecs != null && !_valueFieldSpecs.isEmpty()) {
        ObjectNode valueFieldSpecsNode = JsonUtils.newObjectNode();
        for (Map.Entry<String, FieldSpec> entry : _valueFieldSpecs.entrySet()) {
          valueFieldSpecsNode.set(entry.getKey(), entry.getValue().toJsonObject());
        }
        jsonObject.set("valueFieldSpecs", valueFieldSpecsNode);
      }
      if (_defaultValueFieldSpec != null) {
        jsonObject.set("defaultValueFieldSpec", _defaultValueFieldSpec.toJsonObject());
      }
    } else {
      // STRUCT / MAP / LIST — preserve master's behavior: emit childFieldSpecs unconditionally.
      ObjectNode childFieldSpecsNode = JsonUtils.newObjectNode();
      for (Map.Entry<String, FieldSpec> entry : _childFieldSpecs.entrySet()) {
        childFieldSpecsNode.set(entry.getKey(), entry.getValue().toJsonObject());
      }
      jsonObject.set("childFieldSpecs", childFieldSpecsNode);
    }
    return jsonObject;
  }
}
