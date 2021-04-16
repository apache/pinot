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
package org.apache.pinot.spi.crypt;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotCrypterFactoryTest {
  private static final String CONFIG_SUBSET_KEY = "keyMap";
  private static final String SAMPLE_KEYMAP_VAL = "sample";

  @Test
  public void testDefaultPinotCrypter() {
    PinotCrypterFactory.init(new PinotConfiguration());
    Assert.assertTrue(PinotCrypterFactory.create("NoOpPinotCrypter") instanceof NoOpPinotCrypter);
    Assert.assertTrue(PinotCrypterFactory.create("nooppinotcrypter") instanceof NoOpPinotCrypter);
  }

  @Test
  public void testConfiguredPinotCrypter() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("class.testpinotcrypter", TestPinotCrypter.class.getName());
    properties.put("testpinotcrypter" + "." + CONFIG_SUBSET_KEY, SAMPLE_KEYMAP_VAL);
    PinotCrypterFactory.init(new PinotConfiguration(properties));
    Assert.assertTrue(PinotCrypterFactory.create(NoOpPinotCrypter.class.getSimpleName()) instanceof NoOpPinotCrypter);
    PinotCrypter testPinotCrypter = PinotCrypterFactory.create(TestPinotCrypter.class.getSimpleName());
    Assert.assertTrue(testPinotCrypter instanceof TestPinotCrypter);
    testPinotCrypter.encrypt(null, null);
  }

  public static final class TestPinotCrypter implements PinotCrypter {
    @Override
    public void init(PinotConfiguration config) {
      Assert.assertTrue(config.containsKey(CONFIG_SUBSET_KEY));
      Assert.assertTrue(config.getProperty(CONFIG_SUBSET_KEY).equalsIgnoreCase(SAMPLE_KEYMAP_VAL));
    }

    @Override
    public void encrypt(File decryptedFile, File encryptedFile) {

    }

    @Override
    public void decrypt(File encryptedFile, File decryptedFile) {

    }
  }
}
