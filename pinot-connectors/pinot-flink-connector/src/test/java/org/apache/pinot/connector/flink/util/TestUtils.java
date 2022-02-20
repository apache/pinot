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
package org.apache.pinot.connector.flink.util;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;


public final class TestUtils {

  private TestUtils() {
  }

  public static void ensureDirectoriesExistAndEmpty(@Nonnull File... dirs)
      throws IOException {
    File[] var1 = dirs;
    int var2 = dirs.length;

    for (int var3 = 0; var3 < var2; var3++) {
      File dir = var1[var3];
      FileUtils.deleteDirectory(dir);
      Assert.assertTrue(dir.mkdirs());
    }
  }
}
