/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for the Utils class.
 *
 */
public class RequestUtilsTest {
  @Test(dataProvider = "getAllGroupByColumnsDataProvider")
  public void testGetAllGroupByColumns(GroupBy groupBy, List<String> expectedColumns) {
    List<String> allGroupByColumns = Lists.newArrayList(RequestUtils.getAllGroupByColumns(groupBy));
    Collections.sort(allGroupByColumns);
    Assert.assertEquals(allGroupByColumns, expectedColumns);
  }

  @DataProvider(name = "getAllGroupByColumnsDataProvider")
  public Object[][] getAllGroupByColumnsDataProvider() {
    List<Object[]> entries = new ArrayList<>();
    GroupBy groupBy = null;

    entries.add(new Object[]{
        groupBy, Lists.newArrayList()
      });

    groupBy = new GroupBy();
    groupBy.setColumns(Lists.newArrayList("country"));
    entries.add(new Object[]{
      groupBy, Lists.newArrayList("country")
    });

    groupBy = new GroupBy();
    groupBy.setColumns(Lists.newArrayList("country", "food"));
    entries.add(new Object[]{
      groupBy, Lists.newArrayList("country", "food")
    });

    groupBy = new GroupBy();
    groupBy.setExpressions(Lists.newArrayList("timeConvert(time, 'HOURS', 'DAYS')"));
    entries.add(new Object[]{
      groupBy, Lists.newArrayList("time")
    });

    groupBy = new GroupBy();
    groupBy.setExpressions(Lists.newArrayList("timeConvert(time, 'HOURS', 'DAYS')", "dateTimeConvert(date, 'HOURS', 'DAYS')"));
    entries.add(new Object[]{
      groupBy, Lists.newArrayList("date", "time")
    });

    groupBy = new GroupBy();
    groupBy.setColumns(Lists.newArrayList("country"));
    groupBy.setExpressions(Lists.newArrayList("timeConvert(time, 'HOURS', 'DAYS')"));
    entries.add(new Object[]{
      groupBy, Lists.newArrayList("country", "time")
    });

    groupBy = new GroupBy();
    groupBy.setColumns(Lists.newArrayList("country", "time"));
    groupBy.setExpressions(Lists.newArrayList("timeConvert(time, 'HOURS', 'DAYS')", "dateTimeConvert(date, 'HOURS', 'DAYS')"));
    entries.add(new Object[]{
      groupBy, Lists.newArrayList("country", "date", "time")
    });

    groupBy = new GroupBy();
    entries.add(new Object[]{
      groupBy, Lists.newArrayList()
    });

    return entries.toArray(new Object[entries.size()][]);
  }
}
