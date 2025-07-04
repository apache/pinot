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
package org.apache.pinot.common.catalog;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/// A custom catalog reader that uses a custom name matcher to bypass Calcite's built-in support for case-insensitive
/// schema definition.
///
/// Calcite's built-in support for case-insensitive schema is way too aggressive, leading to all identifiers being
/// transformed to lowercase after the validation stage. This is cumbersome and breaks several funcionalities of Apache
/// Pinot during query processing.
///
/// Because of that, we need to implement a custom catalog reader with a name matcher that although implementing a
/// case-insensitive lookup for schema identifiers makes Calcite unaware of it to avoid the toLowerCase transformation
/// made by the library for its internal memory structures.
public class PinotCatalogReader extends CalciteCatalogReader {
  public PinotCatalogReader(CalciteSchema rootSchema, List<String> defaultSchema,
      RelDataTypeFactory typeFactory, CalciteConnectionConfig config, boolean caseSensitive) {
    super(rootSchema, new PinotNameMatcher(caseSensitive),
        ImmutableList.of(ImmutableList.copyOf(defaultSchema), ImmutableList.of()), typeFactory, config);
  }
}
