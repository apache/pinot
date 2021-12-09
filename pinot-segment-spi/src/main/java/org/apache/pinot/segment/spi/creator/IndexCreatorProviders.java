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
package org.apache.pinot.segment.spi.creator;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Plugin registration point to allow index creation logic to be swapped out.
 * Plugins should not reimplement Pinot's default index creation logic.
 * Users provide an override to Pinot's index creation logic. This is simplified
 * by extending {@see IndexCreatorProviders.Default}
 */
public final class IndexCreatorProviders {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexCreatorProviders.class);

  static final IndexCreatorProvider DEFAULT = defaultProvider();
  private static final AtomicReference<IndexCreatorProvider> REGISTRATION = new AtomicReference<>(DEFAULT);

  private IndexCreatorProviders() {
  }

  /**
   * The caller provides a decorator to wrap the default provider, which allows plugins to create
   * a delegation chain.
   * @param provider index creation override
   * @return true if this is the first invocation and the provider has not yet been used.
   */
  public static boolean registerProvider(IndexCreatorProvider provider) {
    return REGISTRATION.compareAndSet(DEFAULT, provider);
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
    public static final IndexCreatorProvider PROVIDER = REGISTRATION.get();
  }

  private static IndexCreatorProvider defaultProvider() {
    // use MethodHandle to break circular dependency and keep implementation details encapsulated within
    // pinot-segment-local
    String className = "org.apache.pinot.segment.local.segment.creator.impl.DefaultIndexCreatorProvider";
    try {
      Class<?> clazz = Class.forName(className, false, IndexCreatorProviders.class.getClassLoader());
      return (IndexCreatorProvider) MethodHandles.publicLookup()
          .findConstructor(clazz, MethodType.methodType(void.class)).invoke();
    } catch (Throwable implausible) {
      LOGGER.error("could not construct MethodHandle for {}", className,
          implausible);
      throw new IllegalStateException(implausible);
    }
  }

  /**
   * Extend this class to override index creation
   */
  public static class Default implements IndexCreatorProvider {

    @Override
    public BloomFilterCreator newBloomFilterCreator(IndexCreationContext.BloomFilter context)
        throws IOException {
      return DEFAULT.newBloomFilterCreator(context);
    }

    @Override
    public ForwardIndexCreator newForwardIndexCreator(IndexCreationContext.Forward context)
        throws Exception {
      return DEFAULT.newForwardIndexCreator(context);
    }

    @Override
    public GeoSpatialIndexCreator newGeoSpatialIndexCreator(IndexCreationContext.Geospatial context)
        throws IOException {
      return DEFAULT.newGeoSpatialIndexCreator(context);
    }

    @Override
    public DictionaryBasedInvertedIndexCreator newInvertedIndexCreator(IndexCreationContext.Inverted context)
        throws IOException {
      return DEFAULT.newInvertedIndexCreator(context);
    }

    @Override
    public JsonIndexCreator newJsonIndexCreator(IndexCreationContext.Json context)
        throws IOException {
      return DEFAULT.newJsonIndexCreator(context);
    }

    @Override
    public CombinedInvertedIndexCreator newRangeIndexCreator(IndexCreationContext.Range context)
        throws IOException {
      return DEFAULT.newRangeIndexCreator(context);
    }

    @Override
    public TextIndexCreator newFSTIndexCreator(IndexCreationContext.Text context)
        throws IOException {
      return DEFAULT.newFSTIndexCreator(context);
    }
  }
}
