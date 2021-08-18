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
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IngestionJobLauncherTest {

  @Test
  public void testIngestionJobLauncherWithTemplate()
      throws IOException, ClassNotFoundException {
    Map<String, Object> context = GroovyTemplateUtils.getTemplateContext(Arrays.asList("year=2020", "month=05", "day=06"));
    SegmentGenerationJobSpec spec = IngestionJobLauncher
        .getSegmentGenerationJobSpec(GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
            null, context);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/05/06");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/05/06");
  }

  @Test
  public void testIngestionJobLauncherWithUnicodeCharForMultivalueFieldDelimiter()
      throws IOException, ClassNotFoundException {
    SegmentGenerationJobSpec spec = IngestionJobLauncher
        .getSegmentGenerationJobSpec(GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_unicode.yaml").getFile(),
            null, null);
    Assert.assertEquals("\ufff0", spec.getRecordReaderSpec().getConfigs().get("multiValueDelimiter"));
  }

  @Test
  public void testIngestionJobLauncherWithTemplateAndPropertyFile()
      throws IOException, ClassNotFoundException {
    SegmentGenerationJobSpec spec = IngestionJobLauncher
        .getSegmentGenerationJobSpec(GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
            GroovyTemplateUtils.class.getClassLoader().getResource("job.config").getFile(), null);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2019/06/07");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2019/06/07");
  }

  @Test
  public void testIngestionJobLauncherWithTemplateAndPropertyFileAndValueOverride()
      throws IOException, ClassNotFoundException {
    Map<String, Object> context = GroovyTemplateUtils.getTemplateContext(Arrays.asList("year=2020"));
    SegmentGenerationJobSpec spec = IngestionJobLauncher
        .getSegmentGenerationJobSpec(GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
            GroovyTemplateUtils.class.getClassLoader().getResource("job.config").getFile(), context);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/06/07");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/06/07");
    Assert.assertEquals(spec.getSegmentCreationJobParallelism(), 100);
  }

  @Test
  public void testIngestionJobLauncherWithJsonTemplate()
      throws IOException, ClassNotFoundException {
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_json_spec_template.json").getFile(),
        GroovyTemplateUtils.class.getClassLoader().getResource("job_json.config").getFile(), null);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/07/22");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/07/22");
    Assert.assertEquals(spec.getSegmentCreationJobParallelism(), 0);
  }
}
