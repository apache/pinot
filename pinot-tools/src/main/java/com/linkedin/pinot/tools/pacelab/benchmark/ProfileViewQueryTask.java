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

public class ProfileViewQueryTask extends QueryTask {

    public ProfileViewQueryTask(Properties config, String[] queries) {
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

        long maxTimestamp = Long.parseLong(config.getProperty("max_timestamp"));
        long minTimestamp = Long.parseLong(config.getProperty("min_timestamp"));
        int minWeeklyChange = Integer.parseInt(config.getProperty("min_weekly_change"));
        int maxWeeklyChange = Integer.parseInt(config.getProperty("max_weekly_change"));


        long timestampRange = maxTimestamp - minTimestamp + 1;
        int rankingRange = maxWeeklyChange - minWeeklyChange + 1;
        long timestamp = minTimestamp + (int)(Math.random() * timestampRange);

        String query;
        switch (queryId) {
            case 0:
                query = String.format(queries[queryId], timestamp);
                runQuery(query);
                break;
            case 1:
                int ranking = minWeeklyChange + (int)(Math.random() * rankingRange);
                query = String.format(queries[queryId], timestamp, ranking);
                runQuery(query);
                break;
            case 2:
                int lowerBound = minWeeklyChange + (int)(Math.random() * rankingRange);
                int higherBound = minWeeklyChange + (int)(Math.random() * rankingRange);
                if (lowerBound > higherBound) {
                    //swap them
                    int temp = lowerBound;
                    lowerBound = higherBound;
                    higherBound = temp;
                }
                query = String.format(queries[queryId], timestamp, lowerBound, higherBound);
                runQuery(query);
                break;
            case 3:
                query = String.format(queries[queryId], timestamp);
                runQuery(query);
                break;
        }

    }
}
