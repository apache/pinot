/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.integration.tests;

import com.google.common.collect.Lists;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test for the new config format command line tools.
 */
public class NewConfigApplyIntegrationTest extends BaseClusterIntegrationTest {
  @BeforeClass
  public void setUp() throws Exception {
    // Start an empty cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Unpack the table configs if necessary
    List<String> configs = Lists.newArrayList("mytable.conf", "profiles/test1.conf", "profiles/test2.conf");
    List<File> configFiles = configs
        .stream()
        .map(path -> getClass().getClassLoader().getResource(path))
        .map(TestUtils::getFileFromResourceUrl)
        .map(File::new)
        .collect(Collectors.toList());

    // Create a new table using the command line tools
    Process process = new ProcessBuilder("java", "-cp", "pinot-tools/target/pinot-tool-launcher-jar-with-dependencies.jar", "com.linkedin.pinot.tools.admin.PinotAdministrator")
        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .start();
    int returnCode = process.waitFor();
    System.out.println("returnCode = " + returnCode);
    assertEquals(returnCode, 0);
  }

  @Test
  public void testNothing() {

  }
}
