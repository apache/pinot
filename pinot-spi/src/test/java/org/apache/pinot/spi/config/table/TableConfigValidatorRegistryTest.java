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
package org.apache.pinot.spi.config.table;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.spi.exception.ConfigValidationException;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TableConfigValidatorRegistryTest {
  private static final TableConfig DUMMY_TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  @AfterMethod
  public void tearDown() {
    TableConfigValidatorRegistry.reset();
  }

  @Test
  public void testNoOpWhenEmpty() {
    TableConfigValidatorRegistry.validate(DUMMY_TABLE_CONFIG, null);
  }

  @Test(expectedExceptions = ConfigValidationException.class, expectedExceptionsMessageRegExp = "rejected")
  public void testRejectionPropagates() {
    TableConfigValidatorRegistry.register((tableConfig, schema) -> {
      throw new ConfigValidationException("rejected");
    });
    TableConfigValidatorRegistry.validate(DUMMY_TABLE_CONFIG, null);
  }

  @Test
  public void testShortCircuitOnFirstRejection() {
    AtomicBoolean secondCalled = new AtomicBoolean(false);
    TableConfigValidatorRegistry.register((tableConfig, schema) -> {
      throw new ConfigValidationException("first rejects");
    });
    TableConfigValidatorRegistry.register((tableConfig, schema) -> secondCalled.set(true));
    try {
      TableConfigValidatorRegistry.validate(DUMMY_TABLE_CONFIG, null);
      fail("Expected ConfigValidationException");
    } catch (ConfigValidationException e) {
      assertFalse(secondCalled.get());
    }
  }

  @Test
  public void testMultipleValidatorsInvokedInOrder() {
    AtomicBoolean firstCalled = new AtomicBoolean(false);
    AtomicBoolean secondCalled = new AtomicBoolean(false);
    TableConfigValidatorRegistry.register((tableConfig, schema) -> {
      assertFalse(secondCalled.get());
      firstCalled.set(true);
    });
    TableConfigValidatorRegistry.register((tableConfig, schema) -> {
      assertTrue(firstCalled.get());
      secondCalled.set(true);
    });
    TableConfigValidatorRegistry.validate(DUMMY_TABLE_CONFIG, null);
    assertTrue(firstCalled.get());
    assertTrue(secondCalled.get());
  }

  @Test
  public void testResetClearsValidators() {
    TableConfigValidatorRegistry.register((tableConfig, schema) -> {
      throw new ConfigValidationException("should be cleared");
    });
    TableConfigValidatorRegistry.reset();
    TableConfigValidatorRegistry.validate(DUMMY_TABLE_CONFIG, null);
  }
}
