/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.content.EmailContentFormatter;
import com.linkedin.thirdeye.alert.content.HierarchicalAnomaliesEmailContentFormatter;
import com.linkedin.thirdeye.alert.content.MultipleAnomaliesEmailContentFormatter;
import org.junit.Assert;
import org.testng.annotations.Test;


public class TestEmailContentFormatterFactory {

  @Test
  public void testGetEmailContentFormatter() throws Exception{
    EmailContentFormatter emailContentFormatter;
    emailContentFormatter = EmailContentFormatterFactory.fromClassName("MultipleAnomaliesEmailContentFormatter");
    Assert.assertNotNull(emailContentFormatter);
    Assert.assertTrue(emailContentFormatter instanceof MultipleAnomaliesEmailContentFormatter);

    emailContentFormatter = EmailContentFormatterFactory.fromClassName("HierarchicalAnomaliesEmailContentFormatter");
    Assert.assertNotNull(emailContentFormatter);
    Assert.assertTrue(emailContentFormatter instanceof HierarchicalAnomaliesEmailContentFormatter);
  }
}
