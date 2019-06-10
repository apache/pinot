/*
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

package org.apache.pinot.thirdeye.detection.components;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionTestUtils;
import org.apache.pinot.thirdeye.detection.spec.TriggerConditionGrouperSpec;
import org.apache.pinot.thirdeye.detection.spi.exception.DetectorException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.DetectionUtils.*;
import static org.apache.pinot.thirdeye.detection.components.TriggerConditionGrouper.*;


public class TriggerConditionGrouperTest {

  private static final String PROP_VALUE = "value";

  public static MergedAnomalyResultDTO makeAnomaly(long start, long end, String entity) {
    MergedAnomalyResultDTO anomaly = DetectionTestUtils.makeAnomaly(1000l, start, end, null, null, Collections.<String, String>emptyMap());
    Map<String, String> props = new HashMap<>();
    props.put(PROP_DETECTOR_COMPONENT_NAME, entity);
    anomaly.setProperties(props);
    return anomaly;
  }

  /**
   *
   *           0           1000    1500       2000
   *  A        |-------------|      |-----------|
   *
   *                500                       2000     2500      3000
   *  B              |--------------------------|        |---------|
   *
   *                500    1000    1500       2000
   *  A && B         |-------|      |-----------|
   *
   */
  @Test
  public void testAndGrouping() {
    TriggerConditionGrouper grouper = new TriggerConditionGrouper();

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    anomalies.add(makeAnomaly(0, 1000, "entityA"));
    anomalies.add(makeAnomaly(500, 2000, "entityB"));
    anomalies.add(makeAnomaly(1500, 2000, "entityA"));
    anomalies.add(makeAnomaly(2500, 3000, "entityB"));

    TriggerConditionGrouperSpec spec = new TriggerConditionGrouperSpec();
    spec.setOperator(PROP_AND);
    Map<String, Object> leftOp = new HashMap<>();
    leftOp.put(PROP_VALUE, "entityA");
    spec.setLeftOp(leftOp);

    Map<String, Object> rigthOp = new HashMap<>();
    rigthOp.put(PROP_VALUE, "entityB");
    spec.setRightOp(rigthOp);

    grouper.init(spec, null);
    List<MergedAnomalyResultDTO> groupedAnomalies = grouper.group(anomalies);

    Assert.assertEquals(groupedAnomalies.size(), 5);

    int childCounter = 0;
    List<MergedAnomalyResultDTO> parents = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : groupedAnomalies) {
      if (anomaly.isChild()) {
        childCounter++;
      } else {
        parents.add(anomaly);
      }
    }
    Assert.assertEquals(childCounter, 3);
    Assert.assertEquals(parents.size(), 2);

    parents = mergeAndSortAnomalies(parents, null);
    Assert.assertEquals(parents.get(0).getStartTime(), 500);
    Assert.assertEquals(parents.get(0).getEndTime(), 1000);
    Assert.assertEquals(parents.get(1).getStartTime(), 1500);
    Assert.assertEquals(parents.get(1).getEndTime(), 2000);
  }

  /**
   *
   *           0           1000    1500       2000
   *  A        |-------------|      |-----------|
   *
   *                500                       2000     2500      3000
   *  B              |--------------------------|       |---------|
   *
   *           0                              2000     2500      3000
   *  A || B   |--------------------------------|       |---------|
   *
   */
  @Test
  public void testOrGrouping() {
    TriggerConditionGrouper grouper = new TriggerConditionGrouper();

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    anomalies.add(makeAnomaly(0, 1000, "entityA"));
    anomalies.add(makeAnomaly(500, 2000, "entityB"));
    anomalies.add(makeAnomaly(1500, 2000, "entityA"));
    anomalies.add(makeAnomaly(2500, 3000, "entityB"));

    TriggerConditionGrouperSpec spec = new TriggerConditionGrouperSpec();
    spec.setOperator(PROP_OR);
    Map<String, Object> leftOp = new HashMap<>();
    leftOp.put(PROP_VALUE, "entityA");
    spec.setLeftOp(leftOp);

    Map<String, Object> rigthOp = new HashMap<>();
    rigthOp.put(PROP_VALUE, "entityB");
    spec.setRightOp(rigthOp);

    grouper.init(spec, null);
    List<MergedAnomalyResultDTO> groupedAnomalies = grouper.group(anomalies);

    Assert.assertEquals(groupedAnomalies.size(), 6);

    int childCounter = 0;
    List<MergedAnomalyResultDTO> parents = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : groupedAnomalies) {
      if (anomaly.isChild()) {
        childCounter++;
      } else {
        parents.add(anomaly);
      }
    }
    Assert.assertEquals(childCounter, 4);
    Assert.assertEquals(parents.size(), 2);

    parents = mergeAndSortAnomalies(parents, null);
    Assert.assertEquals(parents.get(0).getStartTime(), 0);
    Assert.assertEquals(parents.get(0).getEndTime(), 2000);
    Assert.assertEquals(parents.get(1).getStartTime(), 2500);
    Assert.assertEquals(parents.get(1).getEndTime(), 3000);
  }
}