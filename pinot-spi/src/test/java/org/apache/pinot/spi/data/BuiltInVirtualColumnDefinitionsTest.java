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

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;


public class BuiltInVirtualColumnDefinitionsTest {

  /// The names are declared twice - once as constants in [BuiltInVirtualColumn] and once implicitly by
  /// [BuiltInVirtualColumnDefinitions#DEFINITIONS]. A definition missing from the name set would stop being filtered
  /// out of a segment's physical columns and would be counted as a user dimension by [SchemaInfo].
  @Test
  public void testDefinitionsMatchTheDeclaredNames() {
    assertEquals(BuiltInVirtualColumnDefinitions.NAMES, BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS);
    assertEquals(BuiltInVirtualColumnDefinitions.DEFINITIONS.size(),
        BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS.size(), "Duplicate name in DEFINITIONS");
  }

  /// Every built-in virtual column name must start with `$`: that prefix is what excludes them from `SELECT *` in
  /// both query engines.
  @Test
  public void testNamesAreDollarPrefixed() {
    for (String name : BuiltInVirtualColumnDefinitions.NAMES) {
      assertTrue(name.startsWith("$"), "Built-in virtual column must be $-prefixed: " + name);
    }
  }

  /// Field specs are mutable and are stored by reference in the schema they are added to, so each call must return a
  /// fresh instance - the server side mutates them to attach the provider class and, for `$segmentName`, a per-segment
  /// value.
  @Test
  public void testCreateFieldSpecReturnsFreshInstances() {
    for (BuiltInVirtualColumnDefinitions.Definition definition : BuiltInVirtualColumnDefinitions.DEFINITIONS) {
      DimensionFieldSpec first = definition.createFieldSpec();
      DimensionFieldSpec second = definition.createFieldSpec();
      assertNotSame(first, second, "Field spec must not be shared for: " + definition.getName());
      assertEquals(first, second);
      assertEquals(first.getName(), definition.getName());
      assertEquals(first.getDataType(), definition.getDataType());
      assertEquals(first.isSingleValueField(), definition.isSingleValueField());
    }
  }

  @Test
  public void testAddToSchemaIsIdempotentAndNeverOverwrites() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("test")
        .addSingleValueDimension("dim", FieldSpec.DataType.STRING)
        .build();
    BuiltInVirtualColumnDefinitions.addToSchema(schema);
    Set<String> afterFirst = new HashSet<>(schema.getColumnNames());
    assertTrue(afterFirst.containsAll(BuiltInVirtualColumnDefinitions.NAMES));

    // A second call must not duplicate or replace anything
    BuiltInVirtualColumnDefinitions.addToSchema(schema);
    assertEquals(new HashSet<>(schema.getColumnNames()), afterFirst);

    for (BuiltInVirtualColumnDefinitions.Definition definition : BuiltInVirtualColumnDefinitions.DEFINITIONS) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(definition.getName());
      assertNotNull(fieldSpec);
      assertEquals(fieldSpec.getDataType(), definition.getDataType());
    }
  }

  /// A user-defined column of the same name must win, so that adding a built-in virtual column never silently
  /// changes the type of an existing user column in the broker's schema.
  @Test
  public void testAddToSchemaDoesNotOverrideUserColumn() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("test")
        .addSingleValueDimension(BuiltInVirtualColumn.TOTALDOCS, FieldSpec.DataType.STRING)
        .build();
    BuiltInVirtualColumnDefinitions.addToSchema(schema);
    assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.TOTALDOCS).getDataType(), FieldSpec.DataType.STRING);
  }
}
