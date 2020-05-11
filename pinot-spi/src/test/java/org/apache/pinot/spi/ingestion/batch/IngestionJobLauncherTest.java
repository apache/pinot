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
package org.apache.pinot.spi.ingestion.batch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.JinjaTemplateUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IngestionJobLauncherTest {

  @Test
  public void testIngestionJobLauncherWithTemplate()
      throws IOException {
    Map<String, Object> context =
        JinjaTemplateUtils.getTemplateContext(Arrays.asList("year=2020", "month=05", "day=06"));
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        JinjaTemplateUtils.class.getClassLoader().getResource("ingestionJobSpecTemplate.yaml").getFile(), context);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/05/06");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/05/06");
  }
}
