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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.util.Map;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class MutableOpenStructDataSourceTest {

  private PinotDataBufferMemoryManager _mm;

  @BeforeMethod
  public void setUp() {
    _mm = new DirectMemoryManager("MutableOpenStructDataSourceTest");
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mm.close();
  }

  private ComplexFieldSpec spec() {
    return new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of());
  }

  @Test
  public void testGetDataSourcePerKey()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      DataSource clicks = ds.getDataSource("clicks");
      assertNotNull(clicks);
      assertTrue(ds.isMaterialized("clicks"));
      assertTrue(ds.isFullyMaterialized()); // mutable always holds everything
    }
  }

  @Test
  public void testGetDataSourceForUnknownKey()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 0);
      assertNull(ds.getDataSource("missing"));
      assertFalse(ds.isMaterialized("missing"));
    }
  }

  @Test
  public void testGetDataSourcesReturnsAllKeys()
      throws Exception {
    try (MutableOpenStructIndex idx = new MutableOpenStructIndex("metrics", spec(),
        OpenStructIndexConfig.DEFAULT, _mm, 100)) {
      idx.index(0, Map.of("clicks", 5L, "country", "US"));
      MutableOpenStructDataSource ds = new MutableOpenStructDataSource(spec(), idx, 1);
      assertEquals(ds.getDataSources().size(), 2);
    }
  }
}
