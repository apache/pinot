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
import org.apache.pinot.thirdeye.cube.additive.AdditiveCubeNode;
import org.apache.pinot.thirdeye.cube.additive.AdditiveRow;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdditiveCubeNodeTest {
  // Since CubeNode has cyclic reference between current node and parent node, the toString() will encounter
  // overflowStack exception if it doesn't take care of the cyclic reference carefully.
  @Test
  public void testToString() {
    AdditiveRow root = new AdditiveRow(new Dimensions(), new DimensionValues());
    AdditiveCubeNode rootNode = new AdditiveCubeNode(root);

    AdditiveRow child = new AdditiveRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")), 20, 30);
    AdditiveCubeNode childNode = new AdditiveCubeNode(1, 0, child, rootNode);

    childNode.toString();
  }

  @Test
  public void testEqualsAndHashCode() {
    AdditiveRow root1 = new AdditiveRow(new Dimensions(), new DimensionValues());
    AdditiveCubeNode rootNode1 = new AdditiveCubeNode(root1);

    AdditiveRow root2 = new AdditiveRow(new Dimensions(), new DimensionValues());
    AdditiveCubeNode rootNode2 = new AdditiveCubeNode(root2);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertTrue(CubeNodeUtils.equalHierarchy(rootNode1, rootNode2));
    Assert.assertEquals(rootNode1.hashCode(), rootNode2.hashCode());

    AdditiveRow root3 = new AdditiveRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")));
    CubeNode rootNode3 = new AdditiveCubeNode(root3);
    Assert.assertNotEquals(rootNode1, rootNode3);
    Assert.assertNotEquals(rootNode1.hashCode(), rootNode3.hashCode());
  }
}
