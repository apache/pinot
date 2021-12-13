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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.provider.IndexReaderProvider;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexingOverrides {

  public interface IndexingOverride extends IndexCreatorProvider, IndexReaderProvider {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingOverrides.class);

  private static final IndexCreatorProvider CREATOR_DEFAULTS = createDefaultCreatorProvider();
  private static final IndexReaderProvider READER_DEFAULTS = createDefaultReaderProvider();
  private static final AtomicReference<IndexingOverride> REGISTRATION = new AtomicReference<>(null);


  private IndexingOverrides() {
  }

  /**
   * The caller provides a decorator to wrap the default provider, which allows plugins to create
   * a delegation chain.
   * @param provider indexing override
   * @return true if this is the first invocation and the provider has not yet been used.
   */
  public static boolean registerProvider(IndexingOverride provider) {
    return REGISTRATION.compareAndSet(null, provider);
  }

  /**
   * Gets the registered {@see IndexReaderProvider} or the default if none was registered yet.
   * @return an index reader provier.
   */
  public static IndexReaderProvider getIndexReaderProvider() {
    return Holder.PROVIDER;
  }

  /**
   * Obtain the registered index creator provider. If the user has provided an override, then it will be used instead.
   * If the user has not provided an override yet, then this action will prevent them from doing so.
   * @return the global index provision logic.
   */
  public static IndexCreatorProvider getIndexCreatorProvider() {
    return Holder.PROVIDER;
  }

  private static final class Holder {
    public static final IndexingOverride PROVIDER = Optional.ofNullable(REGISTRATION.get()).orElseGet(Default::new);
  }

  private static IndexCreatorProvider createDefaultCreatorProvider() {
    return invokeDefaultConstructor("org.apache.pinot.segment.local.segment.creator.impl.DefaultIndexCreatorProvider");
  }

  private static IndexReaderProvider createDefaultReaderProvider() {
    return invokeDefaultConstructor("org.apache.pinot.segment.local.segment.index.readers.DefaultIndexReaderProvider");
  }

  @SuppressWarnings("unchecked")
  private static <T> T invokeDefaultConstructor(String className) {
    try {
      Class<?> clazz = Class.forName(className, false, IndexingOverrides.class.getClassLoader());
      return (T) MethodHandles.publicLookup()
          .findConstructor(clazz, MethodType.methodType(void.class)).invoke();
    } catch (Throwable missing) {
      LOGGER.error("could not construct MethodHandle for {}", className, missing);
      return null;
    }
  }

  /**
   * Extend this class to override index creation
   */
  public static class Default implements IndexingOverride {

    @Override
    public BloomFilterCreator newBloomFilterCreator(IndexCreationContext.BloomFilter context)
        throws IOException {
      ensureCreatorPresent();
      return CREATOR_DEFAULTS.newBloomFilterCreator(context);
    }

    @Override
    public ForwardIndexCreator newForwardIndexCreator(IndexCreationContext.Forward context)
        throws Exception {
      ensureCreatorPresent();
      return CREATOR_DEFAULTS.newForwardIndexCreator(context);
    }

    @Override
    public GeoSpatialIndexCreator newGeoSpatialIndexCreator(IndexCreationContext.Geospatial context)
        throws IOException {
      ensureCreatorPresent();
      return CREATOR_DEFAULTS.newGeoSpatialIndexCreator(context);
    }

    @Override
    public DictionaryBasedInvertedIndexCreator newInvertedIndexCreator(IndexCreationContext.Inverted context)
        throws IOException {
      ensureCreatorPresent();
      return CREATOR_DEFAULTS.newInvertedIndexCreator(context);
    }

    @Override
    public JsonIndexCreator newJsonIndexCreator(IndexCreationContext.Json context)
        throws IOException {
      ensureCreatorPresent();
      return CREATOR_DEFAULTS.newJsonIndexCreator(context);
    }

    @Override
    public CombinedInvertedIndexCreator newRangeIndexCreator(IndexCreationContext.Range context)
        throws IOException {
      ensureCreatorPresent();
      return CREATOR_DEFAULTS.newRangeIndexCreator(context);
    }

    @Override
    public TextIndexCreator newTextIndexCreator(IndexCreationContext.Text context)
        throws IOException {
      ensureCreatorPresent();
      return CREATOR_DEFAULTS.newTextIndexCreator(context);
    }

    @Override
    public BloomFilterReader newBloomFilterReader(PinotDataBuffer dataBuffer, boolean onHeap)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newBloomFilterReader(dataBuffer, onHeap);
    }

    @Override
    public ForwardIndexReader<?> newForwardIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newForwardIndexReader(dataBuffer, metadata);
    }

    @Override
    public H3IndexReader newGeospatialIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newGeospatialIndexReader(dataBuffer, metadata);
    }

    @Override
    public InvertedIndexReader<?> newInvertedIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newInvertedIndexReader(dataBuffer, metadata);
    }

    @Override
    public JsonIndexReader newJsonIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newJsonIndexReader(dataBuffer, metadata);
    }

    @Override
    public RangeIndexReader<?> newRangeIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newRangeIndexReader(dataBuffer, metadata);
    }

    @Override
    public SortedIndexReader<?> newSortedIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newSortedIndexReader(dataBuffer, metadata);
    }

    @Override
    public TextIndexReader newFSTIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IOException {
      ensureReaderPresent();
      return READER_DEFAULTS.newFSTIndexReader(dataBuffer, metadata);
    }

    @Override
    public TextIndexReader newTextIndexReader(File file, ColumnMetadata columnMetadata,
        @Nullable Map<String, String> textIndexProperties) {
      ensureReaderPresent();
      return READER_DEFAULTS.newTextIndexReader(file, columnMetadata, textIndexProperties);
    }
    
    private void ensureReaderPresent() {
      if (READER_DEFAULTS == null) {
        throw new UnsupportedOperationException("default implementation not present on classpath");
      }
    }

    private void ensureCreatorPresent() {
      if (CREATOR_DEFAULTS == null) {
        throw new UnsupportedOperationException("default implementation not present on classpath");
      }
    }
  }
}
