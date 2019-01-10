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

package org.apache.pinot.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertGroupAuxiliaryInfoProviderFactoryTest {
  @Test
  public void testFromSpecNull() throws Exception {
    AlertGroupRecipientProviderFactory alertGroupRecipientProviderFactory = new AlertGroupRecipientProviderFactory();
    AlertGroupAuxiliaryInfoProvider recipientProvider = alertGroupRecipientProviderFactory.fromSpec(null);
    Assert.assertEquals(recipientProvider.getClass(), DummyAlertGroupAuxiliaryInfoProvider.class);
  }

  @Test
  public void testDimensionalAlertGroupRecipientProviderCreation() {
    AlertGroupRecipientProviderFactory alertGroupRecipientProviderFactory = new AlertGroupRecipientProviderFactory();
    Map<String, String> spec = new HashMap<>();
    spec.put(AlertGroupRecipientProviderFactory.GROUP_RECIPIENT_PROVIDER_TYPE_KEY, "diMenSionAL");
    AlertGroupAuxiliaryInfoProvider alertGrouper = alertGroupRecipientProviderFactory.fromSpec(spec);
    Assert.assertEquals(alertGrouper.getClass(), DimensionalAlertGroupAuxiliaryRecipientProvider.class);
  }
}
