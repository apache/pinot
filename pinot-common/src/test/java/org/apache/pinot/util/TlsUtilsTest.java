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
package org.apache.pinot.util;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;


public class TlsUtilsTest {

  @Test
  public void testTlsSettingsReference() throws Exception {
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.load(TlsUtilsTest.class.getClassLoader()
        .getResourceAsStream("sample_broker_config.properties"));
    PinotConfiguration configuration = new PinotConfiguration(props);
  }
}
