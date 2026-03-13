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
package org.apache.pinot.common.systemtable;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


public class SystemTableRegistryTest {
  @Test
  public void testRegisterGetAndClose()
      throws Exception {
    SystemTableRegistry registry = new SystemTableRegistry(null, null, null, null);
    AtomicInteger closeCount = new AtomicInteger();
    SystemTableProvider provider = new TestProvider("SYSTEM.FOO", closeCount);
    registry.register(provider);

    assertNotNull(registry.get("system.foo"));
    assertSame(registry.get("SYSTEM.FOO"), provider);
    assertEquals(registry.getProviders().size(), 1);
    assertEquals(closeCount.get(), 0);

    registry.close();
    assertEquals(closeCount.get(), 1);
    assertFalse(registry.isRegistered("system.foo"));
  }

  @Test
  public void testRegisterRejectsInvalidPrefix() {
    SystemTableRegistry registry = new SystemTableRegistry(null, null, null, null);
    TestProvider badProvider = new TestProvider("foo.bar", new AtomicInteger());
    assertThrows(IllegalArgumentException.class, () -> registry.register(badProvider));
  }

  private static final class TestProvider implements SystemTableProvider {
    private final String _tableName;
    private final AtomicInteger _closeCount;
    private final Schema _schema;

    private TestProvider(String tableName, AtomicInteger closeCount) {
      _tableName = tableName;
      _closeCount = closeCount;
      _schema = new Schema.SchemaBuilder().setSchemaName(tableName)
          .addSingleValueDimension("tableName", FieldSpec.DataType.STRING).build();
    }

    @Override
    public String getTableName() {
      return _tableName;
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    @Override
    public TableConfig getTableConfig() {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName(_tableName).build();
    }

    @Override
    public IndexSegment getDataSource() {
      return org.mockito.Mockito.mock(IndexSegment.class);
    }

    @Override
    public void close() {
      _closeCount.incrementAndGet();
    }
  }
}
