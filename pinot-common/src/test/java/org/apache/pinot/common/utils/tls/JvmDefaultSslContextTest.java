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
package org.apache.pinot.common.utils.tls;

import java.lang.reflect.Field;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class JvmDefaultSslContextTest {
  private static final String JVM_TRUST_STORE = "javax.net.ssl.trustStore";

  @AfterMethod
  public void cleanup() throws Exception {
    System.clearProperty(JVM_TRUST_STORE);
    setInitialized(false);
  }

  @Test
  public void initDefaultSslContextShouldNotThrowWhenInitializationFails() throws Exception {
    setInitialized(false);
    System.setProperty(JVM_TRUST_STORE, "/does/not/exist");

    // Should not throw (used from PinotAdministrator static initializer)
    JvmDefaultSslContext.initDefaultSslContext();
    Assert.assertTrue(isInitialized());

    // Subsequent calls should be short-circuited
    JvmDefaultSslContext.initDefaultSslContext();
    Assert.assertTrue(isInitialized());
  }

  private static boolean isInitialized() throws Exception {
    Field f = JvmDefaultSslContext.class.getDeclaredField("_initialized");
    f.setAccessible(true);
    return (boolean) f.get(null);
  }

  private static void setInitialized(boolean value) throws Exception {
    Field f = JvmDefaultSslContext.class.getDeclaredField("_initialized");
    f.setAccessible(true);
    f.set(null, value);
  }
}
