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
package org.apache.pinot.core.query.request.context;

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests query-wide properties derived when the table schema is assigned.
public class QueryContextTest {
  @Test
  public void testVariantColumnPresenceIsCachedWithSchema() {
    QueryContext queryContext = new QueryContext.Builder().build();
    assertFalse(queryContext.hasVariantColumns());

    queryContext.setSchema(new Schema.SchemaBuilder().addSingleValueDimension("name", DataType.STRING).build());
    assertFalse(queryContext.hasVariantColumns());

    queryContext.setSchema(new Schema.SchemaBuilder().addSingleValueDimension("payload", DataType.VARIANT).build());
    assertTrue(queryContext.hasVariantColumns());

    // Replacing the query schema must recompute the cached capability.
    queryContext.setSchema(new Schema.SchemaBuilder().addSingleValueDimension("name", DataType.STRING).build());
    assertFalse(queryContext.hasVariantColumns());
  }
}
