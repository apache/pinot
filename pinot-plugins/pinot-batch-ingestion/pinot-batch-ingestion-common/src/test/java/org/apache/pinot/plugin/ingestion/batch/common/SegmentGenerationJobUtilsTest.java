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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentGenerationJobUtilsTest {

  @Test
  public void testUseGlobalDirectorySequenceId() {
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(null));
    SegmentNameGeneratorSpec spec = new SegmentNameGeneratorSpec();
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(new HashMap<>());
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(ImmutableMap.of("use.global.directory.sequence.id", "false"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(ImmutableMap.of("use.global.directory.sequence.id", "FALSE"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(ImmutableMap.of("use.global.directory.sequence.id", "True"));
    Assert.assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(ImmutableMap.of("local.directory.sequence.id", "true"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(ImmutableMap.of("local.directory.sequence.id", "TRUE"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(ImmutableMap.of("local.directory.sequence.id", "False"));
    Assert.assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
  }
  
}
