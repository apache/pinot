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
package org.apache.pinot.broker.broker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Regression tests for {@link AccessControlFactory#loadFactory(org.apache.pinot.spi.env.PinotConfiguration,
 * org.apache.helix.store.zk.ZkHelixPropertyStore)}. The loadFactory resolution is realm-aware via
 * PluginManager.loadClass, so plugin-realm-resident factories work the same as in-tree ones — but the
 * non-public-constructor contract from the historical Class.forName(...).getDeclaredConstructor()
 * pattern must be preserved.
 */
public class AccessControlFactoryLoaderTest {

  @Test
  public void loadFactoryWithNullClassConfigReturnsAllowAllAccessControlFactory() {
    PinotConfiguration emptyConfig = new PinotConfiguration(Collections.emptyMap());
    AccessControlFactory factory = AccessControlFactory.loadFactory(emptyConfig, null);
    assertTrue(factory instanceof AllowAllAccessControlFactory,
        "Expected AllowAllAccessControlFactory when no `class` config is set, got: " + factory.getClass());
  }

  @Test
  public void loadFactoryWithExplicitInTreeFqcnResolvesViaSystemClassloader() {
    Map<String, Object> props = new HashMap<>();
    props.put(AccessControlFactory.ACCESS_CONTROL_CLASS_CONFIG, AllowAllAccessControlFactory.class.getName());
    AccessControlFactory factory = AccessControlFactory.loadFactory(new PinotConfiguration(props), null);
    assertEquals(factory.getClass(), AllowAllAccessControlFactory.class);
  }

  @Test
  public void loadFactoryWithUnknownClassThrowsRuntimeExceptionWrappingClassNotFound() {
    Map<String, Object> props = new HashMap<>();
    props.put(AccessControlFactory.ACCESS_CONTROL_CLASS_CONFIG, "org.apache.pinot.does.not.Exist");
    try {
      AccessControlFactory.loadFactory(new PinotConfiguration(props), null);
      fail("Expected RuntimeException for unknown class");
    } catch (RuntimeException thrown) {
      Throwable cause = thrown.getCause();
      assertTrue(cause instanceof ClassNotFoundException || cause instanceof NoClassDefFoundError
              || (cause != null && cause.getCause() instanceof ClassNotFoundException),
          "Expected ClassNotFoundException as the root cause, got: " + cause);
    }
  }

  /**
   * Regression test: the historical loader used getDeclaredConstructor() which finds non-public no-arg
   * constructors. The realm-aware migration must preserve that contract or factories with
   * package-private / protected constructors would silently break on upgrade.
   */
  @Test
  public void loadFactoryWithPackagePrivateConstructorSucceeds() {
    Map<String, Object> props = new HashMap<>();
    props.put(AccessControlFactory.ACCESS_CONTROL_CLASS_CONFIG, PackagePrivateConstructorFactory.class.getName());
    AccessControlFactory factory = AccessControlFactory.loadFactory(new PinotConfiguration(props), null);
    assertEquals(factory.getClass(), PackagePrivateConstructorFactory.class);
  }

  /** Test fixture: factory whose no-arg constructor is package-private. */
  public static class PackagePrivateConstructorFactory extends AccessControlFactory {
    PackagePrivateConstructorFactory() {
    }

    @Override
    public org.apache.pinot.broker.api.AccessControl create() {
      throw new UnsupportedOperationException("test fixture");
    }
  }
}
