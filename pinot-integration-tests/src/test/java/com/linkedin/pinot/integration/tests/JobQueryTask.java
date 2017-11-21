package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.tools.admin.command.PostQueryCommand;

import java.util.Properties;

public class JobQueryTask extends QueryTask {

    public JobQueryTask(Properties config, String[] queries) {
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

        long max_timestamp = Long.parseLong(config.getProperty("max_timestamp"));
        long min_timestamp = Long.parseLong(config.getProperty("min_timestamp"));
        int min_experience = Integer.parseInt(config.getProperty("min_experience"));
        int max_experience = Integer.parseInt(config.getProperty("max_experience"));


        long timestampRange = max_timestamp - min_timestamp + 1;
        int experienceRange = max_experience - min_experience + 1;
        long timestamp = min_timestamp + (int)(Math.random() * timestampRange);

        String query;
        switch (queryId) {
            case 0:
                query = String.format(queries[queryId], timestamp);
                runQuery(query);
                break;
            case 1:
                int experience = min_experience + (int)(Math.random() * experienceRange);
                query = String.format(queries[queryId], timestamp, experience);
                runQuery(query);
                break;
            case 2:
                int lowerBound = min_experience + (int)(Math.random() * experienceRange);
                int higherBound = min_experience + (int)(Math.random() * experienceRange);
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
