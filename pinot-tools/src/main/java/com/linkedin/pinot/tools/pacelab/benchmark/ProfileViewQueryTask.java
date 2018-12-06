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

import com.linkedin.pinot.core.data.GenericRow;
import org.apache.commons.lang.math.LongRange;
import org.xerial.util.ZipfRandom;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class ProfileViewQueryTask extends QueryTaskDaemon {
    List<GenericRow> _profileTable;
    
    //ZipfRandom _zipfRandom;
    
    Random _profileIndexGenerator;
    
    

    public ProfileViewQueryTask(Properties config, String[] queries, String dataDir, int testDuration, Criteria pCriteria) {
        setConfig(config);
        setQueries(queries);
        setDataDir(dataDir);
        setTestDuration(testDuration);
        EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir);
        criteria = pCriteria;

//        long minProfileViewStartTime = Long.parseLong(config.getProperty("MinProfileViewStartTime"));
//        long maxProfileViewStartTime = Long.parseLong(config.getProperty("MaxProfileViewStartTime"));
//        double zipfS = Double.parseDouble(config.getProperty("ZipfSParameter"));        
//        int hourCount = (int) Math.ceil((maxProfileViewStartTime-minProfileViewStartTime)/(HourSecond));
//        _zipfRandom = new ZipfRandom(zipfS,hourCount);
        _profileIndexGenerator = new Random(System.currentTimeMillis());

        try
        {
            _profileTable = eventTableGenerator.readProfileTable();

        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        super.run();
    }

    public void generateAndRunQuery(int queryId) throws Exception {
        generateAndRunQuery(queryId,0);
    }

    public void generateAndRunQuery(int queryId, int queryType) throws Exception {
        EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir);
        Properties config = getConfig();
        String[] queries = getQueries();

        int selectLimit = CommonTools.getSelectLimt(config);
        int groupByLimit = Integer.parseInt(config.getProperty(Constant.GROUP_BY_LIMIT));
        
        GenericRow randomProfile = eventTableGenerator.getRandomGenericRow(_profileTable, _profileIndexGenerator);

        String query;
        String clause = criteria.getClause(Constant.VIEW_START_TIME, queryType);
        if(!clause.equals("")){
            clause = " AND " +clause;
        }
        switch (queryId) {
	        case 0:
	            query = String.format(queries[queryId], randomProfile.getValue("ID"),clause);
	            runQuery(query);
	            break;
	
	        case 1:
	            query = String.format(queries[queryId], randomProfile.getValue("ID"), clause, groupByLimit);
	            runQuery(query);
	            break;
	        case 2:
	            query = String.format(queries[queryId], randomProfile.getValue("ID"), clause, groupByLimit);
	            runQuery(query);
	            break;
	        case 3:
	            query = String.format(queries[queryId], randomProfile.getValue("Position"), clause);
	            runQuery(query);
	            break;
	        case 4:
	            query = String.format(queries[queryId], randomProfile.getValue("WorkPlace"), clause);
	            runQuery(query);
	            break;
		}

	}
}
