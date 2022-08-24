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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class IngestionJobLauncherTest {

  private Map<String, String> _defaultEnvironmentValues;

  @BeforeMethod
  public void setup() {
    _defaultEnvironmentValues = new HashMap<String, String>() {{
      put("year", "2022");
      put("month", "08");
    }};
  }
  @Test
  public void testIngestionJobLauncherWithTemplate() {
    Map<String, Object> context =
        GroovyTemplateUtils.getTemplateContext(Arrays.asList("year=2020", "month=05", "day=06"));
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
        null, context, _defaultEnvironmentValues);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/05/06");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/05/06");
    // searchRecursively is set to false in the yaml file.
    Assert.assertFalse(spec.isSearchRecursively());
  }

  @Test
  public void testIngestionJobLauncherWithUnicodeCharForMultivalueFieldDelimiter() {
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_unicode.yaml").getFile(), null,
        null, null);
    Assert.assertEquals("\ufff0", spec.getRecordReaderSpec().getConfigs().get("multiValueDelimiter"));
    // searchRecursively is set to true by default.
    Assert.assertTrue(spec.isSearchRecursively());
  }

  @Test
  public void testIngestionJobLauncherWithTemplateAndPropertyFile() {
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
        GroovyTemplateUtils.class.getClassLoader().getResource("job.config").getFile(), null, null);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2019/06/07");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2019/06/07");
  }

  @Test
  public void testIngestionJobLauncherWithTemplateAndPropertyFileAndValueOverride() {
    Map<String, Object> context = GroovyTemplateUtils.getTemplateContext(Arrays.asList("year=2020"));
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
        GroovyTemplateUtils.class.getClassLoader().getResource("job.config").getFile(), context, null);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/06/07");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/06/07");
    Assert.assertEquals(spec.getSegmentCreationJobParallelism(), 100);
  }

  @Test
  public void testIngestionJobLauncherWithTemplateAndPropertyFileAndEnvironmentVariableOverride() {
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
        GroovyTemplateUtils.class.getClassLoader().getResource("job.config").getFile(), null,
        _defaultEnvironmentValues);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2022/08/07");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2022/08/07");
    Assert.assertEquals(spec.getSegmentCreationJobParallelism(), 100);
  }

  @Test
  public void testIngestionJobLauncherWithTemplateAndPropertyFileAndValueAndEnvironmentVariableOverride() {
    Map<String, Object> context = GroovyTemplateUtils.getTemplateContext(Arrays.asList("year=2020"));
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_spec_template.yaml").getFile(),
        GroovyTemplateUtils.class.getClassLoader().getResource("job.config").getFile(), context,
        _defaultEnvironmentValues);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/08/07");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/08/07");
    Assert.assertEquals(spec.getSegmentCreationJobParallelism(), 100);
  }
  @Test
  public void testIngestionJobLauncherWithJsonTemplate() {
    SegmentGenerationJobSpec spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
        GroovyTemplateUtils.class.getClassLoader().getResource("ingestion_job_json_spec_template.json").getFile(),
        GroovyTemplateUtils.class.getClassLoader().getResource("job_json.config").getFile(), null, null);
    Assert.assertEquals(spec.getInputDirURI(), "file:///path/to/input/2020/07/22");
    Assert.assertEquals(spec.getOutputDirURI(), "file:///path/to/output/2020/07/22");
    Assert.assertEquals(spec.getSegmentCreationJobParallelism(), 0);
  }
}
