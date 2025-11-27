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
package org.apache.pinot.segment.spi.index;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertSame;


public class IndexServiceTest {

  @Test
  public void testPriorities() {
    TestIndexType indexType1 = new TestIndexType("test");
    TestIndexType indexType2 = new TestIndexType("test");

    IndexPlugin<TestIndexType> plugin1 = new IndexPlugin<TestIndexType>() {
      @Override
      public TestIndexType getIndexType() {
        return indexType1;
      }
      @Override
      public int getPriority() {
        return 0;
      }
    };
    IndexPlugin<TestIndexType> plugin2 = new IndexPlugin<TestIndexType>() {
      @Override
      public TestIndexType getIndexType() {
        return indexType2;
      }

      @Override
      public int getPriority() {
        return 1;
      }
    };

    TreeSet<IndexPlugin<?>> ascendingSet = new TreeSet<>(Comparator.comparingInt(IndexPlugin::getPriority));
    ascendingSet.add(plugin1);
    ascendingSet.add(plugin2);
    IndexService indexService1 = new IndexService(ascendingSet);
    assertSame(indexService1.get("test"), indexType2);

    // Verifies that the order in the test doesn't actually matter
    TreeSet<IndexPlugin<?>> descendingSet = new TreeSet<>((p1, p2) -> p2.getPriority() - p1.getPriority());
    descendingSet.add(plugin1);
    descendingSet.add(plugin2);
    IndexService indexService2 = new IndexService(descendingSet);
    assertSame(indexService2.get("test"), indexType2);
  }

  private static class TestIndexType implements IndexType<IndexConfig, IndexReader, IndexCreator> {
    private final String _id;

    public TestIndexType(String id) {
      _id = id;
    }

    @Override
    public String getId() {
      return _id;
    }

    @Override
    public Class<IndexConfig> getIndexConfigClass() {
      return IndexConfig.class;
    }

    @Override
    public IndexConfig getDefaultConfig() {
      return new IndexConfig(true);
    }

    @Override
    public Map<String, IndexConfig> getConfig(TableConfig tableConfig, Schema schema) {
      return Map.of();
    }

    @Override
    public IndexCreator createIndexCreator(IndexCreationContext context, IndexConfig indexConfig)
        throws Exception {
      throw new UnsupportedOperationException("Indexes of type " + getClass().getName() + " should not be created");
    }

    @Override
    public IndexReaderFactory<IndexReader> getReaderFactory() {
      throw new UnsupportedOperationException(IndexType.class.getName() + " should not be read");
    }

    @Override
    public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
      return List.of("test");
    }

    @Override
    public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory,
        Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
      throw new UnsupportedOperationException(IndexType.class.getName() + " should not be created");
    }

    @Override
    public void convertToNewFormat(TableConfig tableConfig, Schema schema) {
    }
  }
}
