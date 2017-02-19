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
package com.linkedin.pinot.core.segment;

import com.linkedin.pinot.core.segment.index.ColumnMetadataTest;

public class SegmentNameGeneratorTest extends ColumnMetadataTest{
  // TODO Fix the tests to run and add the annotations back

  /*
  public void testPostfix() throws Exception {
    ColumnMetadataTest columnMetadataTest = new ColumnMetadataTest();
    // Build the Segment metadata.
    SegmentGeneratorConfig config = columnMetadataTest.CreateSegmentConfigWithoutCreator();
    DefaultSegmentNameConfig
        segmentNameConfig = new DefaultSegmentNameConfig("daysSinceEpoch", "mytable", "postfix");
//    config.setSegmentNameConfig(segmentNameConfig);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
     Assert.assertEquals(driver.getSegmentName(), "mytable_1756015683_1756015683_postfix");
  }

  public void testNullTimeColumn() throws Exception {
    ColumnMetadataTest columnMetadataTest = new ColumnMetadataTest();
    // Build the Segment metadata.
    SegmentGeneratorConfig config = columnMetadataTest.CreateSegmentConfigWithoutCreator();
    DefaultSegmentNameConfig segmentNameConfig = new DefaultSegmentNameConfig(null, "mytable", "postfix");
//    config.setSegmentNameConfig(segmentNameConfig);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    Assert.assertEquals(driver.getSegmentName(), "mytable_postfix");
  }

  public void testAlreadyNamedSegmentUsingDefaultSegmentNameConfig() throws Exception {
    ColumnMetadataTest columnMetadataTest = new ColumnMetadataTest();
    // Build the Segment metadata.
    SegmentGeneratorConfig config = columnMetadataTest.CreateSegmentConfigWithoutCreator();
    DefaultSegmentNameConfig segmentNameConfig = new DefaultSegmentNameConfig("daysSinceEpoch", "mytable", "postfix");
    segmentNameConfig.setSegmentName("ALREADYNAMEDBUTSHOULDBEDEPRECATED");
//    config.setSegmentNameConfig(segmentNameConfig);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    Assert.assertEquals(driver.getSegmentName(), "ALREADYNAMEDBUTSHOULDBEDEPRECATED");
  }

  public void testAlreadyNamedSegmentWithDefaultConstructor() throws Exception {
    ColumnMetadataTest columnMetadataTest = new ColumnMetadataTest();
    // Build the Segment metadata.
    SegmentGeneratorConfig config = columnMetadataTest.CreateSegmentConfigWithoutCreator();
    DefaultSegmentNameConfig segmentNameConfig = new DefaultSegmentNameConfig("ALREADYNAMEDBUTSHOULDBEDEPRECATED");
//    config.setSegmentNameConfig(segmentNameConfig);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    Assert.assertEquals(driver.getSegmentName(), "ALREADYNAMEDBUTSHOULDBEDEPRECATED");
  }

  public void testDefaultSegmentNameConfig() throws Exception {
    ColumnMetadataTest columnMetadataTest = new ColumnMetadataTest();
    // Build the Segment metadata.
    SegmentGeneratorConfig config = columnMetadataTest.createSimpleSegmentConfig();
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    Assert.assertEquals(driver.getSegmentName(), "testTable_");
  }
  */
}
