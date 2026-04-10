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
package org.apache.pinot.spi.config.instance;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.spi.exception.ConfigValidationException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class InstanceConfigValidatorRegistryTest {
  private static final Instance DUMMY_INSTANCE =
      new Instance("localhost", 1234, InstanceType.SERVER, null, null, 0, 0, 0, 0, false);

  @AfterMethod
  public void tearDown() {
    InstanceConfigValidatorRegistry.reset();
  }

  @Test
  public void testNoOpWhenEmpty() {
    InstanceConfigValidatorRegistry.validate(DUMMY_INSTANCE);
  }

  @Test(expectedExceptions = ConfigValidationException.class, expectedExceptionsMessageRegExp = "rejected")
  public void testRejectionPropagates() {
    InstanceConfigValidatorRegistry.register(instance -> {
      throw new ConfigValidationException("rejected");
    });
    InstanceConfigValidatorRegistry.validate(DUMMY_INSTANCE);
  }

  @Test
  public void testShortCircuitOnFirstRejection() {
    AtomicBoolean secondCalled = new AtomicBoolean(false);
    InstanceConfigValidatorRegistry.register(instance -> {
      throw new ConfigValidationException("first rejects");
    });
    InstanceConfigValidatorRegistry.register(instance -> secondCalled.set(true));
    try {
      InstanceConfigValidatorRegistry.validate(DUMMY_INSTANCE);
      fail("Expected ConfigValidationException");
    } catch (ConfigValidationException e) {
      assertFalse(secondCalled.get());
    }
  }

  @Test
  public void testMultipleValidatorsInvokedInOrder() {
    AtomicBoolean firstCalled = new AtomicBoolean(false);
    AtomicBoolean secondCalled = new AtomicBoolean(false);
    InstanceConfigValidatorRegistry.register(instance -> {
      assertFalse(secondCalled.get());
      firstCalled.set(true);
    });
    InstanceConfigValidatorRegistry.register(instance -> {
      assertTrue(firstCalled.get());
      secondCalled.set(true);
    });
    InstanceConfigValidatorRegistry.validate(DUMMY_INSTANCE);
    assertTrue(firstCalled.get());
    assertTrue(secondCalled.get());
  }

  @Test
  public void testResetClearsValidators() {
    InstanceConfigValidatorRegistry.register(instance -> {
      throw new ConfigValidationException("should be cleared");
    });
    InstanceConfigValidatorRegistry.reset();
    InstanceConfigValidatorRegistry.validate(DUMMY_INSTANCE);
  }
}
