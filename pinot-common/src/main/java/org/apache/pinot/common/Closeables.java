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
package org.apache.pinot.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A utility class to help release multiple {@link Closeable}
 * resources in a safe manner without leaking.
 * As an example if we have a list of Closeable resources,
 * then the following code is prone to leaking 1 or more
 * subsequent resources if an exception is thrown while
 * closing one of them.
 *
 * for (closeable_resource : resources) {
 *   closeable_resource.close()
 * }
 *
 * The helper methods provided here do this safely
 * while keeping track of exception(s) raised during
 * close() of each resource and still continuing to close
 * subsequent resources.
 */
public class Closeables {

  private static final Logger LOGGER = LoggerFactory.getLogger(Closeables.class);

  /**
   * Close a collection of {@link Closeable} resources
   * @param closeables collection of resources to close
   * @throws IOException
   */
  public static void close(Iterable<? extends Closeable> closeables)
      throws IOException {

    if (closeables == null) {
      return;
    }

    IOException topLevelException = null;

    for (Closeable closeable : closeables) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (IOException e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else if (e != topLevelException) {
          topLevelException.addSuppressed(e);
        }
      }
    }

    if (topLevelException != null) {
      LOGGER.error("Failed to close resource", topLevelException);
      throw topLevelException;
    }
  }

  /**
   * Another version of {@link Closeables#close(Iterable)} which allows
   * to pass variable number of closeable resources when the caller
   * doesn't already have them in a collection.
   * @param closeables one or more resources to close
   * @throws IOException
   */
  public static void close(Closeable... closeables) throws IOException {
    close(Arrays.asList(closeables));
  }
}
