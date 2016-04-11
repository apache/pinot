/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.segment.index.converter;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentFormatConverterFactoryTest {

  @Test
  public void testGetConverter()
      throws Exception {
    {
      SegmentFormatConverter converter = SegmentFormatConverterFactory.getConverter(SegmentVersion.v1, SegmentVersion.v3);
      Assert.assertEquals(converter.getClass(), SegmentV1V2ToV3FormatConverter.class);
    }
    {
      SegmentFormatConverter converter = SegmentFormatConverterFactory.getConverter(SegmentVersion.v2, SegmentVersion.v3);
      Assert.assertEquals(converter.getClass(), SegmentV1V2ToV3FormatConverter.class);
    }
    {
      SegmentFormatConverter converter = SegmentFormatConverterFactory.getConverter(SegmentVersion.v1, SegmentVersion.v2);
      Assert.assertEquals(converter.getClass(), SegmentFormatConverterV1ToV2.class);
    }
  }
}
