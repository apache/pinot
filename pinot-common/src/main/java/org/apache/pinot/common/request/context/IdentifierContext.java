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
package org.apache.pinot.common.request.context;

import java.util.Objects;
import org.apache.pinot.common.utils.DataSchema;

/**
 * The {@code IdentifierContext} class represents a Identifer in the query.
 * <p> This class includes information that about an identifier. The v1 engine uses column names for identifiers. The
 * multistage query engine uses ordinals to distinctly track each identifier. So this context is set up to support both
 * v1 and multistage engine identifiers.
 */
public class IdentifierContext {
  private DataSchema.ColumnDataType _dataType;
  String _name;
  // Identifier Index is needed because multistage engine tracks identifiers with the index(ordinal) position.
  int _identifierIndex;

  public IdentifierContext(String name, DataSchema.ColumnDataType dataType, int identifierIndex) {
    _name = name;
    _dataType = dataType;
    _identifierIndex = identifierIndex;
  }

  public String getName() {
    return _name;
  }

  public DataSchema.ColumnDataType getDataType() {
    return _dataType;
  }

  public int getIndex() {
    return _identifierIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IdentifierContext)) {
      return false;
    }
    IdentifierContext that = (IdentifierContext) o;
    return Objects.equals(_name, that._name) && Objects.equals(_identifierIndex, that._identifierIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _identifierIndex);
  }

  @Override
  public String toString() {
    return _name;
  }
}
