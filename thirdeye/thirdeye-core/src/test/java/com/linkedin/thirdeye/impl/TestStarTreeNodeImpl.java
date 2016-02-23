package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TestStarTreeNodeImpl {
  private StarTreeConfig config;
  private MetricSchema metricSchema;
  private StarTreeRecordStoreFactory recordStoreFactory;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("SampleConfig.json"));
    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    recordStoreFactory = new StarTreeRecordStoreFactoryLogBufferImpl();
    recordStoreFactory.init(null, config, null);
  }

  @Test
  public void testInducedSplit() throws Exception {
    StarTreeNode root = createRoot();
    root.init(config, recordStoreFactory);

    for (int i = 0; i < 100; i++) {
      MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
      ts.set(0, "M", 1);

      StarTreeRecordImpl.Builder b = new StarTreeRecordImpl.Builder()
          .setDimensionKey(getDimensionKey("A" + (i % 4), "B" + (i % 8), "C" + (i % 16)))
          .setMetricTimeSeries(ts);
      root.getRecordStore().update(b.build(config));

      StringBuilder sb = new StringBuilder();
      sb.append("B").append(i % 8).append("C").append(i % 16);
    }

    MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
    ts.set(0, "M", 0);

    // Add an "other" value (this should not have explict, but should be in other node)
    StarTreeRecord other = new StarTreeRecordImpl.Builder().setDimensionKey(
        getDimensionKey(StarTreeConstants.OTHER, StarTreeConstants.OTHER, StarTreeConstants.OTHER))
        .setMetricTimeSeries(ts).build(config);
    root.getRecordStore().update(other);

    // Split on A (should have 4 children: A0, A1, A2, A3)
    root.split("A");
    Assert.assertEquals(root.getChildren().size(), 4);
    Assert.assertNotNull(root.getOtherNode());
    Assert.assertNotNull(root.getStarNode());
    Assert.assertEquals(root.getChildDimensionName(), "A");

    // Check children
    Set<String> childDimensionValues = new HashSet<String>();
    for (StarTreeNode child : root.getChildren()) {
      Assert.assertEquals(child.getDimensionName(), "A");
      Assert.assertTrue(child.isLeaf());
      childDimensionValues.add(child.getDimensionValue());
    }
    Set<String> expectedValues = new HashSet<String>(Arrays.asList("A0", "A1", "A2", "A3"));
    Assert.assertEquals(childDimensionValues, expectedValues);

    // Check children by explicitly getting
    for (int i = 0; i < 4; i++) {
      Assert.assertNotNull(root.getChild("A" + i));
    }

    // Check invalid child
    Assert.assertNull(root.getChild("A4"));
  }

  private StarTreeNode createRoot() {
    return new StarTreeNodeImpl(UUID.randomUUID(), StarTreeConstants.STAR, StarTreeConstants.STAR,
        new ArrayList<String>(), new HashMap<String, String>(), new HashMap<String, StarTreeNode>(),
        null, null);
  }

  private DimensionKey getDimensionKey(String a, String b, String c) {
    return new DimensionKey(new String[] {
        a, b, c
    });
  }
}
