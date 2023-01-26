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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableJsonIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexingOverrides {

  public interface IndexingOverride extends MutableIndexProvider {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingOverrides.class);

  private static final MutableIndexProvider MUTABLE_INDEX_DEFAULTS = createDefaultMutableIndexProvider();

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
   * Gets the registered {@see MutableIndexProvider} or the default if none was registered yet.
   * @return a mutable index reader provider.
   */
  public static MutableIndexProvider getMutableIndexProvider() {
    return Holder.PROVIDER;
  }

  private static final class Holder {
    public static final IndexingOverride PROVIDER = Optional.ofNullable(REGISTRATION.get()).orElseGet(Default::new);
  }

  private static MutableIndexProvider createDefaultMutableIndexProvider() {
    return invokeDefaultConstructor(
        "org.apache.pinot.segment.local.indexsegment.mutable.DefaultMutableIndexProvider");
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
    public MutableForwardIndex newForwardIndex(MutableIndexContext.Forward context) {
      ensureMutableReaderPresent();
      return MUTABLE_INDEX_DEFAULTS.newForwardIndex(context);
    }

    @Override
    public MutableInvertedIndex newInvertedIndex(MutableIndexContext.Inverted context) {
      ensureMutableReaderPresent();
      return MUTABLE_INDEX_DEFAULTS.newInvertedIndex(context);
    }

    @Override
    public MutableTextIndex newTextIndex(MutableIndexContext.Text context) {
      ensureMutableReaderPresent();
      return MUTABLE_INDEX_DEFAULTS.newTextIndex(context);
    }

    @Override
    public MutableJsonIndex newJsonIndex(MutableIndexContext.Json context) {
      ensureMutableReaderPresent();
      return MUTABLE_INDEX_DEFAULTS.newJsonIndex(context);
    }

    @Override
    public MutableDictionary newDictionary(MutableIndexContext.Dictionary context) {
      ensureMutableReaderPresent();
      return MUTABLE_INDEX_DEFAULTS.newDictionary(context);
    }

    private void ensureMutableReaderPresent() {
      if (MUTABLE_INDEX_DEFAULTS == null) {
        throw new UnsupportedOperationException("default implementation not present on classpath");
      }
    }
  }
}
