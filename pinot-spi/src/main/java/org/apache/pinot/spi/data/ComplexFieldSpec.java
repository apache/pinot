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
 * It could be STRUCT, MAP, LIST or OPEN_STRUCT.
 *
 * Per-type usage of {@code _childFieldSpecs}:
 * <ul>
 *   <li><b>MAP</b>: exactly two reserved entries {@code key} and {@code value}, declaring
 *       the uniform key/value types of the map.</li>
 *   <li><b>STRUCT</b>: arbitrary named subfields, defining the fixed struct schema.</li>
 *   <li><b>LIST</b>: a single entry describing the element type (by convention).</li>
 *   <li><b>OPEN_STRUCT</b>: optional per-key type hints for declared keys (any names).
 *       A required {@link #_defaultValueFieldSpec} provides the fallback type for keys
 *       not present here.</li>
 * </ul>
 *
 * Example STRUCT:
 * <pre>
 *   STRUCT(
 *           name: STRING
 *           age: INT
 *           salary: INT
 *           addresses: LIST(STRUCT(apt: INT, street: STRING, city: STRING, zip: INT))
 *         )
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ComplexFieldSpec extends FieldSpec {
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  private final Map<String, FieldSpec> _childFieldSpecs;

  @Nullable
  private final FieldSpec _defaultValueFieldSpec;

  // Default constructor required by JSON de-serializer
  public ComplexFieldSpec() {
    this((FieldSpec) null);
  }

  @JsonCreator
  ComplexFieldSpec(@JsonProperty("defaultValueFieldSpec") @Nullable FieldSpec defaultValueFieldSpec) {
    super();
    _childFieldSpecs = new HashMap<>();
    _defaultValueFieldSpec = defaultValueFieldSpec;
  }

  public ComplexFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      Map<String, FieldSpec> childFieldSpecs) {
    this(name, dataType, isSingleValueField, childFieldSpecs, null);
  }

  public ComplexFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      @Nullable Map<String, FieldSpec> childFieldSpecs, @Nullable FieldSpec defaultValueFieldSpec) {
    super(name, dataType, isSingleValueField);
    Preconditions.checkArgument(
        dataType == DataType.STRUCT || dataType == DataType.MAP
            || dataType == DataType.LIST || dataType == DataType.OPEN_STRUCT,
        "ComplexFieldSpec dataType must be STRUCT, MAP, LIST, or OPEN_STRUCT (got %s)", dataType);
    if (dataType == DataType.OPEN_STRUCT) {
      Preconditions.checkArgument(defaultValueFieldSpec != null,
          "DataType.OPEN_STRUCT requires defaultValueFieldSpec");
    } else {
      Preconditions.checkArgument(defaultValueFieldSpec == null,
          "DataType.%s does not support defaultValueFieldSpec (OPEN_STRUCT only)", dataType);
    }
    _childFieldSpecs = childFieldSpecs == null ? new HashMap<>() : new HashMap<>(childFieldSpecs);
    _defaultValueFieldSpec = defaultValueFieldSpec;
  }

  /// Overrides {@link FieldSpec#setDataType} to enforce per-type invariants on the JSON
  /// deserialization path. The canonical constructor enforces these directly; Jackson uses
  /// the no-arg + setter path, where {@code setDataType} is the final hook at which the
  /// {@code defaultValueFieldSpec} is visible.
  @Override
  public void setDataType(DataType dataType) {
    super.setDataType(dataType);
    if (dataType == DataType.OPEN_STRUCT) {
      Preconditions.checkArgument(_defaultValueFieldSpec != null,
          "DataType.OPEN_STRUCT requires defaultValueFieldSpec");
    } else {
      Preconditions.checkArgument(_defaultValueFieldSpec == null,
          "DataType.%s does not support defaultValueFieldSpec (OPEN_STRUCT only)", dataType);
    }
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
   * Exposes the per-key declared types (from {@code childFieldSpecs}) and the required fallback
   * {@code defaultValueFieldSpec}.
   */
  public static class OpenStructFieldSpec {
    private final String _fieldName;
    private final Map<String, FieldSpec> _valueFieldSpecs;
    private final FieldSpec _defaultValueFieldSpec;

    private OpenStructFieldSpec(ComplexFieldSpec complexFieldSpec) {
      Preconditions.checkArgument(complexFieldSpec.getDataType() == DataType.OPEN_STRUCT,
          "OpenStructFieldSpec view requires OPEN_STRUCT (got %s)", complexFieldSpec.getDataType());
      _fieldName = complexFieldSpec.getName();
      _valueFieldSpecs = Map.copyOf(complexFieldSpec.getChildFieldSpecs());
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
        openStructFieldSpec.getValueFieldSpecs(), openStructFieldSpec.getDefaultValueFieldSpec());
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
    ObjectNode childFieldSpecsNode = JsonUtils.newObjectNode();
    for (Map.Entry<String, FieldSpec> entry : _childFieldSpecs.entrySet()) {
      childFieldSpecsNode.set(entry.getKey(), entry.getValue().toJsonObject());
    }
    jsonObject.set("childFieldSpecs", childFieldSpecsNode);
    if (_defaultValueFieldSpec != null) {
      jsonObject.set("defaultValueFieldSpec", _defaultValueFieldSpec.toJsonObject());
    }
    return jsonObject;
  }
}
