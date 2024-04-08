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
package org.apache.pinot.calcite.jdbc;

import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.pinot.common.function.FunctionRegistry;


/**
 * This class is used to create a {@link CalciteSchema} with a given {@link Schema} as the root.
 */
public class CalciteSchemaBuilder {
  private CalciteSchemaBuilder() {
  }

  /**
   * Creates a {@link CalciteSchema} with a given {@link Schema} as the root.
   *
   * @param root schema to use as a root schema
   * @return calcite schema with given schema as the root
   */
  public static CalciteSchema asRootSchema(Schema root, String name) {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false, name, root);
    SchemaPlus schemaPlus = rootSchema.plus();
    for (Map.Entry<String, List<Function>> e : FunctionRegistry.getRegisteredCalciteFunctionMap().entrySet()) {
      for (Function f : e.getValue()) {
        schemaPlus.add(e.getKey(), f);
      }
    }
    return rootSchema;
  }
}
