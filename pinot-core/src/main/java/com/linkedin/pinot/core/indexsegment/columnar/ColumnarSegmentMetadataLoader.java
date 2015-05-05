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
package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 17, 2014
 *
 */
public class ColumnarSegmentMetadataLoader implements SegmentMetadataLoader {

  public static final Logger LOGGER = LoggerFactory.getLogger(ColumnarSegmentMetadataLoader.class);

  @Override
  public SegmentMetadata loadIndexSegmentMetadataFromDir(String segmentDir) throws Exception {
    return load(new File(segmentDir));
  }

  @Override
  public SegmentMetadata load(File segmentDir) throws Exception {
    final SegmentMetadata segmentMetadata = new SegmentMetadataImpl(segmentDir);
    LOGGER.info("Loaded segment metadata for segment : " + segmentMetadata.getName());
    return segmentMetadata;
  }
}
