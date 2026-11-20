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
package org.apache.pinot.query.type;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.MapSqlType;


/// Calcite type of an OPEN_STRUCT column.
///
/// It is a `MAP<VARCHAR, VARCHAR>` so that `col['key']` validates as a map lookup, and it carries the column's
/// declared child field specs as Calcite types. The `ITEM` operator uses a key's declared type or VARCHAR for an
/// undeclared key. Selecting the whole column returns JSON text.
///
/// The declared types are part of the digest, so two OPEN_STRUCT columns with different declarations are different
/// types after canonicalization. Instances are immutable.
public class OpenStructSqlType extends MapSqlType {
  private final Map<String, RelDataType> _declaredKeyTypes;

  public OpenStructSqlType(RelDataType keyType, RelDataType valueType, boolean isNullable,
      Map<String, RelDataType> declaredKeyTypes) {
    super(keyType, valueType, isNullable);
    _declaredKeyTypes = Collections.unmodifiableMap(new TreeMap<>(declaredKeyTypes));
    // MapSqlType computed the digest before the declared types were set; include them now.
    computeDigest();
  }

  /// Declared type of `key`, or null when the key is not declared.
  @Nullable
  public RelDataType getDeclaredKeyType(String key) {
    return _declaredKeyTypes.get(key);
  }

  public Map<String, RelDataType> getDeclaredKeyTypes() {
    return _declaredKeyTypes;
  }

  @Override
  public boolean deepEquals(@Nullable Object obj) {
    return obj instanceof OpenStructSqlType other && super.deepEquals(other)
        && Objects.equals(_declaredKeyTypes, other._declaredKeyTypes);
  }

  @Override
  public int deepHashCode() {
    return 31 * super.deepHashCode() + Objects.hashCode(_declaredKeyTypes);
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("OPEN_STRUCT");
    // Null while the super constructor computes its first digest.
    if (_declaredKeyTypes != null && !_declaredKeyTypes.isEmpty()) {
      sb.append('(');
      boolean first = true;
      for (Map.Entry<String, RelDataType> entry : _declaredKeyTypes.entrySet()) {
        if (!first) {
          sb.append(", ");
        }
        first = false;
        // A key can contain a type name, a separator, or a quote.
        sb.append('"').append(entry.getKey().replace("\"", "\"\"")).append("\" ")
            .append(withDetail ? entry.getValue().getFullTypeString() : entry.getValue().toString());
      }
      sb.append(')');
    }
  }
}
