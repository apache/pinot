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
package org.apache.pinot.query.catalog;

import com.google.common.base.Preconditions;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.spi.data.Schema;


/**
 * Wrapper for pinot internal info for a table.
 *
 * <p>This construct is used to connect a Pinot table to Apache Calcite's relational planner by providing a
 * {@link RelDataType} of the table to the planner.
 */
public class PinotTable extends AbstractTable implements ScannableTable {
  private final Schema _schema;
  private final boolean _nullHandlingEnabled;

  public PinotTable(Schema schema, boolean nullHandlingEnabled) {
    _schema = schema;
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    Preconditions.checkState(relDataTypeFactory instanceof TypeFactory);
    TypeFactory typeFactory = (TypeFactory) relDataTypeFactory;
    return typeFactory.createRelDataTypeFromSchema(_schema, _nullHandlingEnabled);
  }

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return null;
  }
}
