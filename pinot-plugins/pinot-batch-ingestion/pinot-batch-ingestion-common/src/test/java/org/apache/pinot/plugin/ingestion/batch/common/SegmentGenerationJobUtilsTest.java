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

package org.apache.pinot.plugin.ingestion.batch.common;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentGenerationJobUtilsTest {

  @Test
  public void testResolveAndPersistIngestionGroovyPolicy() {
    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    ExecutionFrameworkSpec executionFrameworkSpec = new ExecutionFrameworkSpec();
    executionFrameworkSpec.setExtraConfigs(Map.of(CommonConstants.Groovy.DISABLE_INGESTION_GROOVY, "false"));
    spec.setExecutionFrameworkSpec(executionFrameworkSpec);

    Assert.assertFalse(SegmentGenerationJobUtils.resolveAndPersistIngestionGroovyPolicy(spec));
    Assert.assertFalse(SegmentGenerationJobUtils.isIngestionGroovyDisabled(spec));
    Assert.assertEquals(spec.getExecutionFrameworkSpec().getExtraConfigs(),
        Map.of(CommonConstants.Groovy.DISABLE_INGESTION_GROOVY, "false"));
  }

  @Test
  public void testMissingPolicyIsCapturedForLaunchAndFailsClosedOnLegacyWorker() {
    SegmentGenerationJobSpec launchSpec = new SegmentGenerationJobSpec();
    boolean expectedLaunchPolicy = FunctionEvaluatorFactory.isIngestionGroovyDisabled();

    Assert.assertEquals(SegmentGenerationJobUtils.resolveAndPersistIngestionGroovyPolicy(launchSpec),
        expectedLaunchPolicy);
    Assert.assertEquals(launchSpec.getExecutionFrameworkSpec().getExtraConfigs().get(
        CommonConstants.Groovy.DISABLE_INGESTION_GROOVY), Boolean.toString(expectedLaunchPolicy));

    SegmentGenerationJobSpec legacyWorkerSpec = new SegmentGenerationJobSpec();
    Assert.assertTrue(SegmentGenerationJobUtils.isIngestionGroovyDisabled(legacyWorkerSpec));
    ExecutionFrameworkSpec invalidExecutionFrameworkSpec = new ExecutionFrameworkSpec();
    invalidExecutionFrameworkSpec.setExtraConfigs(
        Map.of(CommonConstants.Groovy.DISABLE_INGESTION_GROOVY, "invalid"));
    legacyWorkerSpec.setExecutionFrameworkSpec(invalidExecutionFrameworkSpec);
    Assert.assertTrue(SegmentGenerationJobUtils.isIngestionGroovyDisabled(legacyWorkerSpec));
  }

  @Test
  public void testUseGlobalDirectorySequenceId() {
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(null));
    SegmentNameGeneratorSpec spec = new SegmentNameGeneratorSpec();
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(new HashMap<>());
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "false"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "FALSE"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "True"));
    Assert.assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "true"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "TRUE"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "False"));
    Assert.assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
  }
}
