/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package org.apache.pinot.thirdeye.cube.data.node;

import java.util.Collections;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.ratio.RatioCubeNode;
import org.apache.pinot.thirdeye.cube.ratio.RatioRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RatioCubeNodeTest {
  // Since CubeNode has cyclic reference between current node and parent node, the toString() will encounter
  // overflowStack exception if it doesn't take care of the cyclic reference carefully.
  @Test
  public void testToString() {
    RatioRow root = new RatioRow(new Dimensions(), new DimensionValues());
    RatioCubeNode rootNode = new RatioCubeNode(root);

    RatioRow child = new RatioRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")));
    child.setBaselineNumeratorValue(20);
    child.setBaselineDenominatorValue(20);
    child.setCurrentNumeratorValue(30);
    child.setCurrentDenominatorValue(31);
    RatioCubeNode childNode = new RatioCubeNode(1, 0, child, rootNode);

    System.out.println(childNode.toString());
  }

  @Test
  public void testEqualsAndHashCode() {
    RatioRow root1 = new RatioRow(new Dimensions(), new DimensionValues());
    CubeNode rootNode1 = new RatioCubeNode(root1);

    RatioRow root2 = new RatioRow(new Dimensions(), new DimensionValues());
    CubeNode rootNode2 = new RatioCubeNode(root2);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertTrue(CubeNodeUtils.equalHierarchy(rootNode1, rootNode2));
    Assert.assertEquals(rootNode1.hashCode(), rootNode2.hashCode());

    RatioRow root3 = new RatioRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")));
    CubeNode rootNode3 = new RatioCubeNode(root3);
    Assert.assertNotEquals(rootNode1, rootNode3);
    Assert.assertNotEquals(rootNode1.hashCode(), rootNode3.hashCode());
  }
}
