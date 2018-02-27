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
package com.linkedin.pinot.broker.routing.builder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test that random routing tables are really random.
 */
public class BalancedRandomRoutingTableBuilderTest {
  @Test
  public void isRandom() {
    // Build dummy external view
    BalancedRandomRoutingTableBuilder routingTableBuilder = new BalancedRandomRoutingTableBuilder();
    List<InstanceConfig> instanceConfigList = new ArrayList<>();
    ExternalView externalView = new ExternalView("dummy");
    externalView.setState("segment_1", "Server_1.2.3.4_1234", "ONLINE");
    externalView.setState("segment_1", "Server_1.2.3.5_2345", "ONLINE");
    externalView.setState("segment_1", "Server_1.2.3.6_3456", "ONLINE");
    externalView.setState("segment_2", "Server_1.2.3.4_1234", "ONLINE");
    externalView.setState("segment_2", "Server_1.2.3.5_2345", "ONLINE");
    externalView.setState("segment_2", "Server_1.2.3.6_3456", "ONLINE");
    externalView.setState("segment_3", "Server_1.2.3.4_1234", "ONLINE");
    externalView.setState("segment_3", "Server_1.2.3.5_2345", "ONLINE");
    externalView.setState("segment_3", "Server_1.2.3.6_3456", "ONLINE");

    // Create configs for above instances.
    instanceConfigList.add(new InstanceConfig("Server_1.2.3.4_1234"));
    instanceConfigList.add(new InstanceConfig("Server_1.2.3.5_2345"));
    instanceConfigList.add(new InstanceConfig("Server_1.2.3.6_3456"));

    // Build routing table

    routingTableBuilder.computeRoutingTableFromExternalView("dummy", externalView, instanceConfigList);

    List<Map<String, List<String>>> routingTables = routingTableBuilder.getRoutingTables();
    // Check that at least two routing tables are different
    Iterator<Map<String, List<String>>> routingTableIterator = routingTables.iterator();
    Map<String, List<String>> previous = routingTableIterator.next();
    while (routingTableIterator.hasNext()) {
      Map<String, List<String>> current = routingTableIterator.next();
      if (!current.equals(previous)) {
        return;
      }
    }

    Assert.fail("All routing tables are equal!");
  }
}
