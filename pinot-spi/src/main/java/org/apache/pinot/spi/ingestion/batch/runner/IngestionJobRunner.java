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
package org.apache.pinot.spi.ingestion.batch.runner;

import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;


/**
 * IngestionJobRunner is the interface of ingestion job runner.
 *
 * For batch ingestion jobs, there are three types:
 *    SegmentGenerationJobRunner
 *    SegmentTarPushJobRunner
 *    SegmentUriPushJobRunner
 *    SegmentMetadataPushJobRunner
 *
 */
public interface IngestionJobRunner {

  /**
   * Initialize IngestionJobRunner with SegmentGenerationJobSpec
   *
   * @param jobSpec
   */
  void init(SegmentGenerationJobSpec jobSpec);

  /**
   * Run IngestionJobRunner
   *
   * @throws Exception
   */
  void run()
      throws Exception;
}
