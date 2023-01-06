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
package org.apache.pinot.segment.local.startree.v2.builder;

import java.util.Iterator;
import org.apache.pinot.segment.local.startree.CachedStarTreeNode;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CachedStarTreeNodeTest {
  @Test
  public void testGetFields() {
    StarTreeNode childNode = createStarTreeNode(1, 2, -1, 3, 4, 5, 0, true, null);
    StarTreeNode parentNode = createStarTreeNode(2, 20, 1, 1, 2, 6, 1, false, childNode);

    // Parent node
    StarTreeNode cached = new CachedStarTreeNode(parentNode);
    Assert.assertEquals(cached.getDimensionId(), 2);
    Assert.assertEquals(cached.getDimensionValue(), 20);
    Assert.assertEquals(cached.getChildDimensionId(), 1);
    Assert.assertEquals(cached.getStartDocId(), 1);
    Assert.assertEquals(cached.getEndDocId(), 2);
    Assert.assertEquals(cached.getAggregatedDocId(), 6);
    Assert.assertEquals(cached.getNumChildren(), 1);
    Assert.assertFalse(cached.isLeaf());

    // Leaf node
    cached = cached.getChildrenIterator().next();
    Assert.assertTrue(cached instanceof CachedStarTreeNode);
    Assert.assertEquals(cached.getDimensionId(), 1);
    Assert.assertEquals(cached.getDimensionValue(), 2);
    Assert.assertEquals(cached.getChildDimensionId(), -1);
    Assert.assertEquals(cached.getStartDocId(), 3);
    Assert.assertEquals(cached.getEndDocId(), 4);
    Assert.assertEquals(cached.getAggregatedDocId(), 5);
    Assert.assertEquals(cached.getNumChildren(), 0);
    Assert.assertTrue(cached.isLeaf());
  }

  private StarTreeNode createStarTreeNode(int dimId, int dimValue, int childDimId, int startDocId, int endDocId,
      int aggDocId, int numChildren, boolean isLeaf, StarTreeNode childNode) {
    return new StarTreeNode() {
      @Override
      public int getDimensionId() {
        return dimId;
      }

      @Override
      public int getDimensionValue() {
        return dimValue;
      }

      @Override
      public int getChildDimensionId() {
        return childDimId;
      }

      @Override
      public int getStartDocId() {
        return startDocId;
      }

      @Override
      public int getEndDocId() {
        return endDocId;
      }

      @Override
      public int getAggregatedDocId() {
        return aggDocId;
      }

      @Override
      public int getNumChildren() {
        return numChildren;
      }

      @Override
      public boolean isLeaf() {
        return isLeaf;
      }

      @Override
      public StarTreeNode getChildForDimensionValue(int dimensionValue) {
        return null;
      }

      @Override
      public Iterator<? extends StarTreeNode> getChildrenIterator() {
        return new Iterator<>() {
          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public StarTreeNode next() {
            return childNode;
          }
        };
      }
    };
  }
}
