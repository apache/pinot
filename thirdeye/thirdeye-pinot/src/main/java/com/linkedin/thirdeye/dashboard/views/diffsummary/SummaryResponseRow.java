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

package com.linkedin.thirdeye.dashboard.views.diffsummary;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


/**
 * A POJO for front-end representation.
 */
public class SummaryResponseRow extends BaseResponseRow {
  public List<String> names;
  public String otherDimensionValues;
  public double cost;

  public static SummaryResponseRow buildNotAvailableRow() {
    SummaryResponseRow row = new SummaryResponseRow();
    row.names = new ArrayList<>();
    row.names.add(SummaryResponse.NOT_AVAILABLE);
    row.percentageChange = SummaryResponse.NOT_AVAILABLE;
    row.contributionChange = SummaryResponse.NOT_AVAILABLE;
    row.contributionToOverallChange = SummaryResponse.NOT_AVAILABLE;
    row.otherDimensionValues = SummaryResponse.NOT_AVAILABLE;
    return row;
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
