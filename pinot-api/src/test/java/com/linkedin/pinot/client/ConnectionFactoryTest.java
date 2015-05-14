/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.client;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the connection factory
 *
 * @author jfim
 */
public class ConnectionFactoryTest {
  @Test
  public void testZkConnection() {
    // TODO Write test
    // Create a dummy Helix structure
    // Create the connection
    // Check that the broker list has the right length and has the same servers
  }

  @Test
  public void testPropertiesConnection() {
    // TODO Write test
    // Create a properties object
    // Create the connection
    // Check that the broker list has the right length and has the same servers
  }

  @Test
  public void testBrokerList() {
    // Create the connection
    String broker1 = "127.0.0.1:1234";
    String broker2 = "localhost:2345";
    Connection connection = ConnectionFactory.fromHostList(broker1, broker2);

    // Check that the broker list has the right length and has the same servers
    List<String> brokers = new ArrayList<String>();
    brokers.add(broker1);
    brokers.add(broker2);
    Assert.assertEquals(connection.getBrokerList(), brokers);
  }
}
