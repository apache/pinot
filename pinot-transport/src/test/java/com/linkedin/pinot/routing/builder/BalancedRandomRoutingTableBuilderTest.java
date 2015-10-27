package com.linkedin.pinot.routing.builder;

import com.linkedin.pinot.routing.ServerToSegmentSetMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    // Build routing table
    List<ServerToSegmentSetMap> routingTable =
        routingTableBuilder.computeRoutingTableFromExternalView("dummy", externalView, instanceConfigList);

    // Check that at least two routing tables are different
    Iterator<ServerToSegmentSetMap> routingTableIterator = routingTable.iterator();
    ServerToSegmentSetMap previous = routingTableIterator.next();
    while (routingTableIterator.hasNext()) {
      ServerToSegmentSetMap current = routingTableIterator.next();
      System.out.println("current = " + current);
      System.out.println("previous = " + previous);
      if (!current.equals(previous)) {
        // Routing tables differ, test is successful
        return;
      }
    }

    Assert.fail("All routing tables are equal!");
  }
}
