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

package com.linkedin.pinot.tools.pacelab.benchmark;

public class Constant {

	// Table specific constants (to generate where clause randomly)
	public static final String VIEW_START_TIME = "ViewStartTime";
	public static final String READ_START_TIME = "ReadStartTime";
	public static final String APPLY_START_TIME = "ApplyStartTime";
	public static final String CLICK_TIME = "ClickTime";
	public static final String SEARCH_TIME = "SearchTime";

	public static final String MIN_PROFILE_START_TIME = "MinProfileViewStartTime";
	public static final String MAX_PROFILE_START_TIME = "MaxProfileViewStartTime";
	public static final String MIN_APPLY_START_TIME = "MinApplyStartTime";
	public static final String MAX_APPLY_START_TIME = "MaxApplyStartTime";
	public static final String MIN_READ_START_TIME = "MinReadStartTime";
	public static final String MAX_READ_START_TIME = "MaxReadStartTime";
	public static final String MIN_SEARCH_START_TIME = "MinSearchStartTime";
	public static final String MAX_SEARCH_START_TIME = "MaxSearchStartTime";
	public static final String MIN_CLICK_TIME = "MinClickTime";
	public static final String MAX_CLICK_TIME = "MaxClickTime";

	public static final int STOP = -1;
	public static final int QUERY_COUNT = 5;
	public static final int QUERY_TYPE_COUNT = 4;
	public static final int HOURSECOND = 3600;
	public static final int[] SECS_IN_DURATION = {0, HOURSECOND, HOURSECOND*24, HOURSECOND*24*7};

	public static final String GROUP_BY_LIMIT = "GroupByLimit";
	public static final String ZIPFS_PARAMETER = "ZipfSParameter";
	public static final String QUERY_TYPE = "QueryType";
	public static final String QPS = "QPS";

}
