package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
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
  private StarTreeRecordThresholdFunction thresholdFunction;
  private StarTreeRecordStoreFactory recordStoreFactory;

  @BeforeClass
  public void beforeClass()
  {
    thresholdFunction = null; // all pass
    recordStoreFactory = new StarTreeRecordStoreFactoryByteBufferImpl();
    recordStoreFactory.init(Arrays.asList("A", "B", "C"), Arrays.asList("M"), null);
  }

  @Test
  public void testInducedSplit() throws Exception
  {

    StarTreeConfig config = new StarTreeConfig.Builder()
            .setDimensionNames(Arrays.asList("A", "B", "C"))
            .setMetricNames(Arrays.asList("M"))
            .build();

    StarTreeNode root = createRoot();
    root.init(config);

    for (int i = 0; i < 100; i++)
    {
      StarTreeRecordImpl.Builder b = new StarTreeRecordImpl.Builder();
      b.setDimensionValue("A", "A" + (i % 4));
      b.setDimensionValue("B", "B" + (i % 8));
      b.setDimensionValue("C", "C" + (i % 16)); // highest cardinality
      b.setMetricValue("M", 1L);
      b.setTime(0L);
      root.getRecordStore().update(b.build());

      StringBuilder sb = new StringBuilder();
      sb.append("B").append(i % 8)
        .append("C").append(i % 16);
    }

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
            thresholdFunction,
            recordStoreFactory,
            StarTreeConstants.STAR,
            StarTreeConstants.STAR,
            new ArrayList<String>(),
            new HashMap<String, StarTreeNode>(),
            null,
            null);
  }
}
