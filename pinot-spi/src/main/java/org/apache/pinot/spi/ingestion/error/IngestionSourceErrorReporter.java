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
package org.apache.pinot.spi.ingestion.error;

import java.util.function.Consumer;


/// Lets a stream plugin report a source failure it swallows on purpose, such as a metadata call it answers with a
/// partial result, so that the Pinot site that called the plugin can still classify it.
///
/// The site installs a listener around its call into the plugin with [#listen]. [#report] hands the failure to the
/// listener installed on the calling thread, and does nothing when there is none. A reported failure counts as Pinot
/// having given up on the source, so report only what the plugin won't retry itself.
public final class IngestionSourceErrorReporter {
  private static final ThreadLocal<Consumer<Throwable>> LISTENER = new ThreadLocal<>();

  private IngestionSourceErrorReporter() {
  }

  /// Reports a source failure the caller swallows. Never throws, apart from the JVM's own fatal errors.
  public static void report(Throwable failure) {
    Consumer<Throwable> listener = LISTENER.get();
    if (listener != null) {
      try {
        listener.accept(failure);
      } catch (Throwable t) {
        IngestionSourceErrorClassifier.rethrowIfFatal(t);
      }
    }
  }

  /// Installs `listener` for the calling thread until the returned scope is closed, which restores the previous one.
  public static Scope listen(Consumer<Throwable> listener) {
    Consumer<Throwable> previous = LISTENER.get();
    LISTENER.set(listener);
    return () -> {
      if (previous != null) {
        LISTENER.set(previous);
      } else {
        LISTENER.remove();
      }
    };
  }

  /// A listener installed by [#listen].
  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }
}
