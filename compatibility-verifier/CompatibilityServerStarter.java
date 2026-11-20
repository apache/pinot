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
package org.apache.pinot.compat;

import java.lang.reflect.InvocationTargetException;


/**
 * Initializes Lucene on the main thread before starting a compatibility server, including older releases that lack
 * the startup fix. This stateless entry point must run before any concurrent Lucene use in the same JVM.
 */
public final class CompatibilityServerStarter {
  private CompatibilityServerStarter() {
  }

  public static void main(String[] args)
      throws Throwable {
    // Lucene 9.12.0 has a class initialization cycle between IndexWriter, TestSecrets, and ConcurrentMergeScheduler.
    // Resolve it on one thread before Helix starts loading immutable indexes and creating realtime indexes in parallel.
    // https://github.com/apache/lucene/issues/12419
    Class.forName("org.apache.lucene.index.IndexWriter");
    try {
      // Use the distribution's administrator and dependencies, never the current checkout's Pinot implementation.
      Class.forName("org.apache.pinot.tools.admin.PinotAdministrator").getMethod("main", String[].class)
          .invoke(null, (Object) args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
