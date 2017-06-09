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
package com.linkedin.pinot.broker.pruner;

import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides the ZK metadata based segment pruning service.
 */
public class SegmentZKMetadataPrunerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentZKMetadataPrunerService.class);
  List<SegmentZKMetadataPruner> _pruners;

  /**
   * Constructor for the class.
   *
   * @param prunerNames Names of pruners to register with the service.
   */
  public SegmentZKMetadataPrunerService(@Nonnull String[] prunerNames) {
    _pruners = new ArrayList<>(prunerNames.length);

    for (String prunerName : prunerNames) {
      LOGGER.info("Adding segment ZK metadata based pruner: '{}'", prunerName);
      _pruners.add(SegmentZKMetadataPrunerProvider.getSegmentPruner(prunerName));
    }
  }

  /**
   * This method applies all registered pruners on a given segment metadata.
   * Returns true if any of the pruners deems the segment prune-able, false otherwise.
   *
   * @param metadata Segment ZK metadata
   * @param prunerContext Pruner context
   * @return True if segment can be pruned, false otherwise.
   */
  public boolean prune(SegmentZKMetadata metadata, SegmentPrunerContext prunerContext) {
    for (SegmentZKMetadataPruner pruner : _pruners) {
      if (pruner.prune(metadata, prunerContext)) {
        LOGGER.debug("Pruned segment '{}'", metadata.getSegmentName());
        return true;
      }
    }
    return false;
  }
}
