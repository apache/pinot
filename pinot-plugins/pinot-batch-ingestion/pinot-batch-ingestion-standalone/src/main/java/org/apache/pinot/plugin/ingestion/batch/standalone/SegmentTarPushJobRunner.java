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
package org.apache.pinot.plugin.ingestion.batch.standalone;

import java.util.ArrayList;
import java.util.Map;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.plugin.ingestion.batch.common.BaseSegmentPushJobRunner;
import org.apache.pinot.segment.local.utils.ConsistentDataPushUtils;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;

public class SegmentTarPushJobRunner extends BaseSegmentPushJobRunner {

  public SegmentTarPushJobRunner() {
  }

  public SegmentTarPushJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  /**
   * Initialize SegmentTarPushJobRunner with SegmentGenerationJobSpec
   * Checks for required parameters in the spec and enablement of consistent data push.
   * This overrides the init method in BaseSegmentPushJobRunner as the push job spec is required in the base class.
   */
  @Override
  public void init(SegmentGenerationJobSpec spec) {
    _spec = spec;

    // Read Table spec
    if (_spec.getTableSpec() == null) {
      throw new RuntimeException("Missing tableSpec");
    }

    // Read Table config
    if (_spec.getTableSpec().getTableConfigURI() != null) {
      _tableConfig =
          SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI(), spec.getAuthToken());
      _consistentPushEnabled = ConsistentDataPushUtils.consistentDataPushEnabled(_tableConfig);
    }
  }

  public void uploadSegments(Map<String, String> segmentsUriToTarPathMap)
      throws AttemptsExceededException, RetriableOperationException {
    SegmentPushUtils.pushSegments(_spec, _outputDirFS, new ArrayList<>(segmentsUriToTarPathMap.values()));
  }
}
