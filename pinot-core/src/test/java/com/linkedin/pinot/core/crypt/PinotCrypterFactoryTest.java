/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.crypt;

import java.io.File;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotCrypterFactoryTest {
  private static final String CONFIG_SUBSET_KEY = "keyMap";
  private static final String SAMPLE_KEYMAP_VAL = "sample";

  @Test
  public void testDefaultPinotCrypter() {
    PinotCrypterFactory.init(new PropertiesConfiguration());
    Assert.assertTrue(PinotCrypterFactory.create("NoOpPinotCrypter") instanceof NoOpPinotCrypter);
    Assert.assertTrue(PinotCrypterFactory.create("nooppinotcrypter") instanceof NoOpPinotCrypter);
  }

  @Test
  public void testConfiguredPinotCrypter() {
    PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
    propertiesConfiguration.addProperty("class.testpinotcrypter", TestPinotCrypter.class.getName());
    propertiesConfiguration.addProperty("testpinotcrypter" + "." + CONFIG_SUBSET_KEY, SAMPLE_KEYMAP_VAL);
    PinotCrypterFactory.init(propertiesConfiguration);
    Assert.assertTrue(PinotCrypterFactory.create(NoOpPinotCrypter.class.getSimpleName()) instanceof NoOpPinotCrypter);
    PinotCrypter testPinotCrypter = PinotCrypterFactory.create(TestPinotCrypter.class.getSimpleName());
    Assert.assertTrue(testPinotCrypter instanceof TestPinotCrypter);
    testPinotCrypter.encrypt(null, null);
  }

  public static final class TestPinotCrypter implements PinotCrypter {
    @Override
    public void init(Configuration config) {
      Assert.assertTrue(config.containsKey(CONFIG_SUBSET_KEY));
      Assert.assertTrue(config.getString(CONFIG_SUBSET_KEY).equalsIgnoreCase(SAMPLE_KEYMAP_VAL));
    }

    @Override
    public void encrypt(File decryptedFile, File encryptedFile) {

    }

    @Override
    public void decrypt(File encryptedFile, File decryptedFile) {

    }
  }
}
