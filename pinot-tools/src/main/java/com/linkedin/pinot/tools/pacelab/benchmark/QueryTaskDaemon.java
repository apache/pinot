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

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTaskDaemon extends QueryTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(QueryTaskDaemon.class);
	protected AtomicInteger[] threadQueryType;
	protected int threadId = -1;

	public void setThreadQueryType(AtomicInteger[] threadQueryType) {
		this.threadQueryType = threadQueryType;
	}

	@Override
	public void run() {
		if(threadId ==-1) {
			super.run();
			return;
		}

		long timeBeforeSendingQuery;
		long timeAfterSendingQuery;
		long timeDistance;
		long runStartMillisTime = System.currentTimeMillis();
		long currentTimeMillisTime =  System.currentTimeMillis();
		long secondsPassed = (currentTimeMillisTime-runStartMillisTime)/1000;
		int queryType;
		while(secondsPassed < _testDuration && !Thread.interrupted())
		{
			try {
				queryType = threadQueryType[threadId].get();
				if( queryType != Constant.STOP) {
					timeBeforeSendingQuery = System.currentTimeMillis();
					generateAndRunQuery(rand.nextInt(Constant.QUERY_COUNT),queryType);
					timeAfterSendingQuery = System.currentTimeMillis();
					timeDistance = timeAfterSendingQuery - timeBeforeSendingQuery;
					if (timeDistance < 1000) {
						Thread.sleep(1000 - timeDistance);
					}
				}
				else
					Thread.sleep(100);

				currentTimeMillisTime =  System.currentTimeMillis();
				secondsPassed = (currentTimeMillisTime-runStartMillisTime)/1000;
			} catch (Exception e) {
				LOGGER.error("Exception in thread");
			}

		}
	}
	public void generateAndRunQuery(int queryId, int queryType) throws Exception {

	}

	public void setThreadId(int threadId) {
		this.threadId = threadId;
	}
}
