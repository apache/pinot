package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
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

public class TestStarTreeNodeImpl
{
  private StarTreeRecordStoreFactory recordStoreFactory;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    StarTreeConfig config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("SampleConfig.json"));
    recordStoreFactory = new StarTreeRecordStoreFactoryLogBufferImpl();
    recordStoreFactory.init(null, config, null);
  }

  @Test
  public void testInducedSplit() throws Exception
  {

    StarTreeConfig config = new StarTreeConfig.Builder()
            .setCollection("dummy")
            .setDimensions(Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C")))
            .setMetrics(Arrays.asList(new MetricSpec("M", MetricType.INT)))
            .build();

    StarTreeNode root = createRoot();
    root.init(config, recordStoreFactory);

    for (int i = 0; i < 100; i++)
    {
      StarTreeRecordImpl.Builder b = new StarTreeRecordImpl.Builder();
      b.setDimensionValue("A", "A" + (i % 4));
      b.setDimensionValue("B", "B" + (i % 8));
      b.setDimensionValue("C", "C" + (i % 16)); // highest cardinality
      b.setMetricValue("M", 1);
      b.setMetricType("M", MetricType.INT);
      b.setTime(0L);
      root.getRecordStore().update(b.build());

      StringBuilder sb = new StringBuilder();
      sb.append("B").append(i % 8)
        .append("C").append(i % 16);
    }

    // Add an "other" value (this should not have explict, but should be in other node)
    StarTreeRecord other = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", StarTreeConstants.OTHER)
            .setDimensionValue("B", StarTreeConstants.OTHER)
            .setDimensionValue("C", StarTreeConstants.OTHER)
            .setMetricValue("M", 0)
            .setMetricType("M", MetricType.INT)
            .setTime(0L)
            .build();
    root.getRecordStore().update(other);

    // Split on A (should have 4 children: A0, A1, A2, A3)
    root.split("A");
    Assert.assertEquals(root.getChildren().size(), 4);
    Assert.assertNotNull(root.getOtherNode());
    Assert.assertNotNull(root.getStarNode());
    Assert.assertEquals(root.getChildDimensionName(), "A");

    // Check children
    Set<String> childDimensionValues = new HashSet<String>();
    for (StarTreeNode child : root.getChildren())
    {
      Assert.assertEquals(child.getDimensionName(), "A");
      Assert.assertTrue(child.isLeaf());
      childDimensionValues.add(child.getDimensionValue());
    }
    Set<String> expectedValues = new HashSet<String>(Arrays.asList("A0", "A1", "A2", "A3"));
    Assert.assertEquals(childDimensionValues, expectedValues);

    // Check children by explicitly getting
    for (int i = 0; i < 4; i++)
    {
      Assert.assertNotNull(root.getChild("A" + i));
    }

    // Check invalid child
    Assert.assertNull(root.getChild("A4"));
  }

  private StarTreeNode createRoot()
  {
    return new StarTreeNodeImpl(
            UUID.randomUUID(),
            StarTreeConstants.STAR,
            StarTreeConstants.STAR,
            new ArrayList<String>(),
            new HashMap<String, String>(),
            new HashMap<String, StarTreeNode>(),
            null,
            null);
  }
}
