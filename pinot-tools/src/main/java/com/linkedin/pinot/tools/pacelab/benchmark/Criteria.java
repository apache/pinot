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

import java.util.Properties;

import org.apache.commons.lang.math.LongRange;
import org.xerial.util.ZipfRandom;

public class Criteria
{
	private Long maxStartTime;
	private Long minStartTime;
	private ZipfRandom[] zipfRandom;

	public Criteria(Properties config, String pMaxStartTime, String pMinStartTime) {
		minStartTime = Long.parseLong(config.getProperty(pMinStartTime));
		maxStartTime = Long.parseLong(config.getProperty(pMaxStartTime));
		zipfRandom = new ZipfRandom[Constant.QUERY_TYPE_COUNT];
		double zipfS = Double.parseDouble(config.getProperty(Constant.ZIPFS_PARAMETER));
		int count;
		for(int queryType =1; queryType < Constant.QUERY_TYPE_COUNT ; queryType++) {
			count = (int) Math.ceil((maxStartTime-minStartTime)/(Constant.SECS_IN_DURATION[queryType]));
			zipfRandom[queryType] = new ZipfRandom(zipfS,count);
		}
	}

	public String getClause(String column,int pQueryType) {
		 if (pQueryType == 0)
			 return "";
		 LongRange timeRange = getTimeRange(pQueryType);
		 return column + " > " + timeRange.getMinimumLong() +" AND " + column + " < "+timeRange.getMaximumLong();
	}

	private LongRange getTimeRange(int pQueryType) {
		 int duration = zipfRandom[pQueryType].nextInt();
		 long queriedEndTime = maxStartTime;
		 long queriedStartTime = Math.max(minStartTime,queriedEndTime - duration * Constant.SECS_IN_DURATION[pQueryType]);
		 return new LongRange(queriedStartTime,queriedEndTime);
	}

}