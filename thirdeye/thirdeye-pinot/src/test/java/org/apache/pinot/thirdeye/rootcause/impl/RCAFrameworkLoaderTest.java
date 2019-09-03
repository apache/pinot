/*
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

package org.apache.pinot.thirdeye.rootcause.impl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RCAFrameworkLoaderTest {
  @Test
  public void testAugmentPathProperty() {
    File rcaConfig = new File("/my/path/config.yml");

    Map<String, Object> prop = new HashMap<>();
    prop.put("key", "value");
    prop.put("absolutePath", "/absolute/path.txt");
    prop.put("relativePath", "relative_path.txt");
    prop.put("path", "another_relative_path.txt");

    Map<String, Object> aug = RCAFrameworkLoader.augmentPathProperty(prop, rcaConfig);

    Assert.assertEquals(aug.get("key"), "value");
    Assert.assertEquals(aug.get("absolutePath"), "/absolute/path.txt");
    Assert.assertEquals(aug.get("relativePath"), "/my/path/relative_path.txt");
    Assert.assertEquals(aug.get("path"), "/my/path/another_relative_path.txt");
  }
}
