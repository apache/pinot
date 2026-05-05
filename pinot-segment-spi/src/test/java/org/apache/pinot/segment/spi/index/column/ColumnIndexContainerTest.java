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
package org.apache.pinot.segment.spi.index.column;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.VectorIndexConfigProvider;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ColumnIndexContainer.FromMap} vector-index config propagation.
 */
public class ColumnIndexContainerTest {

  @Test
  public void testFromMapUsesProviderBackedMapConfig() {
    VectorIndexConfig config = createVectorConfig("IVF_PQ");
    ProviderBackedMap readers = new ProviderBackedMap(config);
    readers.put(mockIndexType(), new NoOpIndexReader());

    ColumnIndexContainer.FromMap container = new ColumnIndexContainer.FromMap(readers);

    Assert.assertSame(container.getVectorIndexConfig(), config);
  }

  @Test
  public void testBuilderWithAllUsesProviderBackedMapConfig() {
    VectorIndexConfig config = createVectorConfig("IVF_PQ");
    ProviderBackedMap readers = new ProviderBackedMap(config);
    readers.put(mockIndexType(), new NoOpIndexReader());
    ColumnIndexContainer.FromMap container = new ColumnIndexContainer.FromMap.Builder()
        .withAll(readers)
        .build();

    Assert.assertSame(container.getVectorIndexConfig(), config);
  }

  @Test
  public void testBuilderExplicitVectorConfigOverridesMissingReaderConfig() {
    VectorIndexConfig config = createVectorConfig("IVF_FLAT");
    ColumnIndexContainer.FromMap container = new ColumnIndexContainer.FromMap.Builder()
        .withVectorIndexConfig(config)
        .with(mockIndexType(), new NoOpIndexReader())
        .build();

    Assert.assertSame(container.getVectorIndexConfig(), config);
  }

  private static VectorIndexConfig createVectorConfig(String backend) {
    return new VectorIndexConfig(false, backend, 8, 1, VectorIndexConfig.VectorDistanceFunction.COSINE,
        Map.of("vectorIndexType", backend));
  }

  @SuppressWarnings("unchecked")
  private static IndexType<?, IndexReader, ?> mockIndexType() {
    return (IndexType<?, IndexReader, ?>) org.mockito.Mockito.mock(IndexType.class);
  }

  private static final class ProviderBackedMap extends HashMap<IndexType, IndexReader>
      implements VectorIndexConfigProvider {
    private final VectorIndexConfig _vectorIndexConfig;

    private ProviderBackedMap(VectorIndexConfig vectorIndexConfig) {
      _vectorIndexConfig = vectorIndexConfig;
    }

    @Override
    public VectorIndexConfig getVectorIndexConfig() {
      return _vectorIndexConfig;
    }
  }

  private static final class NoOpIndexReader implements IndexReader {
    @Override
    public void close()
        throws IOException {
    }
  }
}
