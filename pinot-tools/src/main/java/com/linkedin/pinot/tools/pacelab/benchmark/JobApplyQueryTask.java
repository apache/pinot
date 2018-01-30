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

import com.linkedin.pinot.tools.pacelab.benchmark.QueryTask;

import java.util.Properties;

public class JobApplyQueryTask extends QueryTask {

    public JobApplyQueryTask(Properties config, String[] queries) {
        setConfig(config);
        setQueries(queries);
    }

    @Override
    public void run() {
        super.run();
    }


    public void generateAndRunQuery(int queryId) throws Exception {
        Properties config = getConfig();
        String[] queries = getQueries();

        long maxApplyStartTime= Long.parseLong(config.getProperty("minApplyStartTime"));
        long minApplyStartTime = Long.parseLong(config.getProperty("maxApplyStartTime"));

        String query = "";
        switch (queryId) {
            case 0:
                //query = String.format(queries[queryId], "");
                runQuery(query);
                break;
            case 1:
                //query = String.format(queries[queryId], );
                runQuery(query);
                break;
            case 2:
                //query = String.format(queries[queryId], );
                runQuery(query);
                break;
            case 3:
                //query = String.format(queries[queryId], );
                runQuery(query);
                break;
        }

    }
}
