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


public class Closeables {

  public static void close(Iterable<? extends Closeable> ac)
      throws IOException {

    if (ac == null) {
      return;
    } else if (ac instanceof Closeable) {
      ((Closeable) ac).close();
      return;
    }

    IOException topLevelException = null;

    for (Closeable closeable : ac) {
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
      throw topLevelException;
    }
  }
  public static void close(Closeable... closeables) throws IOException {
    close(Arrays.asList(closeables));
  }
}
