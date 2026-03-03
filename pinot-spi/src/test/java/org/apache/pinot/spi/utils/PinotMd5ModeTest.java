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
package org.apache.pinot.spi.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PinotMd5ModeTest {
  @Test
  public void testSetterGetter() {
    try {
      PinotMd5Mode.setPinotMd5Disabled(true);
      assertTrue(PinotMd5Mode.isPinotMd5Disabled());
      PinotMd5Mode.setPinotMd5Disabled(false);
      assertFalse(PinotMd5Mode.isPinotMd5Disabled());
    } finally {
      PinotMd5Mode.setPinotMd5Disabled(false);
    }
  }

  @Test
  public void testResetFromSystemProperty() {
    String originalValue = System.getProperty(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED);
    try {
      System.setProperty(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED, "true");
      PinotMd5Mode.resetFromSystemProperty();
      assertTrue(PinotMd5Mode.isPinotMd5Disabled());

      System.setProperty(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED, "false");
      PinotMd5Mode.resetFromSystemProperty();
      assertFalse(PinotMd5Mode.isPinotMd5Disabled());
    } finally {
      if (originalValue != null) {
        System.setProperty(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED, originalValue);
      } else {
        System.clearProperty(CommonConstants.CONFIG_OF_PINOT_MD5_DISABLED);
      }
      PinotMd5Mode.resetFromSystemProperty();
    }
  }
}
