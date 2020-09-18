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

package org.apache.pinot.thirdeye.notification.formatter;

import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.content.templates.HierarchicalAnomaliesContent;
import org.apache.pinot.thirdeye.notification.content.templates.MetricAnomaliesContent;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAlertContentFormatterFactory {

  @Test
  public void testGetEmailContentFormatter() throws Exception{
    BaseNotificationContent alertContent;
    alertContent = new AlertContentFormatterFactory<MetricAnomaliesContent>().fromClassPath(MetricAnomaliesContent.class
    .getCanonicalName());
    Assert.assertNotNull(alertContent);
    Assert.assertTrue(alertContent instanceof MetricAnomaliesContent);

    alertContent = new AlertContentFormatterFactory<MetricAnomaliesContent>().fromClassPath(
        HierarchicalAnomaliesContent.class.getCanonicalName());
    Assert.assertNotNull(alertContent);
    Assert.assertTrue(alertContent instanceof HierarchicalAnomaliesContent);
  }
}
