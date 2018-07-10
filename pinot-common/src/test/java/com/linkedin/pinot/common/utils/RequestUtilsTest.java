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
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for the Utils class.
 *
 */
public class RequestUtilsTest {
  @Test(dataProvider = "getAllGroupByColumnsDataProvider")
  public void testGetAllGroupByColumns(GroupBy groupBy, Set<String> expectedColumns) {
    Assert.assertEquals(RequestUtils.getAllGroupByColumns(groupBy), expectedColumns);
  }

  @DataProvider(name = "getAllGroupByColumnsDataProvider")
  public Object[][] getAllGroupByColumnsDataProvider() {
    List<Object[]> entries = new ArrayList<>();

    entries.add(new Object[]{null, Collections.emptySet()});

    GroupBy groupBy = new GroupBy();
    groupBy.setExpressions(Collections.singletonList("timeConvert(time, 'HOURS', 'DAYS')"));
    entries.add(new Object[]{groupBy, Collections.singleton("time")});

    groupBy = new GroupBy();
    groupBy.setExpressions(
        Arrays.asList("timeConvert(time, 'HOURS', 'DAYS')", "dateTimeConvert(date, 'HOURS', 'DAYS')"));
    entries.add(new Object[]{groupBy, new HashSet<>(Arrays.asList("date", "time"))});

    groupBy = new GroupBy();
    groupBy.setExpressions(Arrays.asList("country", "timeConvert(time, 'HOURS', 'DAYS')"));
    entries.add(new Object[]{groupBy, new HashSet<>(Arrays.asList("country", "time"))});

    groupBy = new GroupBy();
    groupBy.setExpressions(
        Arrays.asList("country", "timeConvert(time, 'HOURS', 'DAYS')", "dateTimeConvert(date, 'HOURS', 'DAYS')",
            "time"));
    entries.add(new Object[]{groupBy, new HashSet<>(Arrays.asList("country", "time", "date"))});

    return entries.toArray(new Object[entries.size()][]);
  }
}
