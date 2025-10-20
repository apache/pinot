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
  private Schema _schema;
  private boolean _excludeVirtualColumns = false;

  public PinotTable(Schema schema) {
    _schema = schema;
  }

  /**
   * Sets whether virtual columns should be excluded from the table schema.
   * This is typically used for NATURAL JOIN operations where virtual columns
   * should not participate in join condition matching.
   */
  public void setExcludeVirtualColumns(boolean excludeVirtualColumns) {
    _excludeVirtualColumns = excludeVirtualColumns;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    TypeFactory typeFactory;
    if (relDataTypeFactory instanceof TypeFactory) {
      typeFactory = (TypeFactory) relDataTypeFactory;
    } else { // this can happen when using Frameworks.withPrepare, which wraps our factory in a JavaTypeFactoryImpl
      typeFactory = TypeFactory.INSTANCE;
    }

    if (shouldExcludeVirtualColumns()) {
      return typeFactory.createRelDataTypeFromSchemaExcludingVirtualColumns(_schema);
    } else {
      return typeFactory.createRelDataTypeFromSchema(_schema);
    }
  }

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return null;
  }
  
  private boolean shouldExcludeVirtualColumns() {
    return _excludeVirtualColumns;
  }
}
