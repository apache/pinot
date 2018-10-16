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

import java.util.concurrent.atomic.AtomicBoolean;

public class QueryTaskDaemon extends QueryTask {

    protected  AtomicBoolean[] thread_status;
    protected int thread_id;

    public void setThread_status(AtomicBoolean[] thread_status) {
        this.thread_status = thread_status;
    }

    @Override
    public void run() {

//        float[] likelihood = getLikelihoodArrayFromProps();
        while(!Thread.interrupted())
        {
            try {
            if(thread_status[thread_id].get()) {
                long timeBeforeSendingQuery = System.currentTimeMillis();
                //TODO: call queries based
//                float randomLikelihood = rand.nextFloat();
//                for (int i = 0; i < likelihood.length; i++)
//                {
//                    if (randomLikelihood <= likelihood[i])
//                    {
//                        generateAndRunQuery(i);
//                        break;
//                    }
//                }
                //TODO: Currently Hardcoded , will modify later on.
                generateAndRunQuery(rand.nextInt(5));
                long timeAfterSendingQuery = System.currentTimeMillis();
                long timeDistance = timeAfterSendingQuery - timeBeforeSendingQuery;
                if (timeDistance < 1000) {
                    Thread.sleep(1000 - timeDistance);
                }
            }
            else
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public void setThread_id(int thread_id) {
        this.thread_id = thread_id;
    }
}
