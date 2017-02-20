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

import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadataTest;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DefaultSegmentNameGeneratorTest extends ColumnMetadataTest{

  @Test
  public void testPostfix() throws Exception {
    ColumnMetadataTest columnMetadataTest = new ColumnMetadataTest();
    // Build the Segment metadata.
    SegmentGeneratorConfig config = columnMetadataTest.CreateSegmentConfigWithoutCreator();
    SegmentNameGenerator segmentNameGenerator = new DefaultSegmentNameGenerator("daysSinceEpoch", null, "mytable", "1");
    config.setSegmentNameGenerator(segmentNameGenerator);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
     Assert.assertEquals(driver.getSegmentName(), "mytable_1756015683_1756015683_1");
  }

  @Test
  public void testAlreadyNamedSegment() throws Exception {
    ColumnMetadataTest columnMetadataTest = new ColumnMetadataTest();
    // Build the Segment metadata.
    SegmentGeneratorConfig config = columnMetadataTest.CreateSegmentConfigWithoutCreator();
    SegmentNameGenerator segmentNameGenerator = new DefaultSegmentNameGenerator("mytable_1");
    config.setSegmentNameGenerator(segmentNameGenerator);
    SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    Assert.assertEquals(driver.getSegmentName(), "mytable_1");
  }

}
