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

public class ProfileViewQueryExecutor extends QueryExecutor{
    private static ProfileViewQueryExecutor instance;
    private final String CONFIG_FILE = "ProfileViewConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {/*
                "SELECT COUNT(*) FROM ProfileView" +
                    " WHERE ViewStartTime > %d AND ViewStartTime < %d",
                "SELECT * FROM ProfileView" +
                    " WHERE ViewStartTime > %d AND ViewStartTime < %d AND ViewedProfileId = '%s' LIMIT %d",*/
		/*"SELECT COUNT(*) FROM ProfileView" + 
		    					" WHERE ViewedProfileId = '%s'"+
		    					"%s",
		    					
    					
			"SELECT ViewerPosition, COUNT(*) FROM ProfileView" +
							" WHERE ViewedProfileId = '%s'"+
							"%s"+
							" GROUP BY ViewerPosition TOP %d",
					
			"SELECT ViewerWorkPlace, COUNT(*) FROM ProfileView" +
							" WHERE ViewedProfileId = '%s'"+
							"%s"+
							" GROUP BY ViewerWorkPlace TOP %d",
							
			"SELECT COUNT(*), FROM ProfileView" +
                        " WHERE ViewedProfilePosition = '%s'"+
                        "%s",
                        
            	"SELECT COUNT(*), AVG(ReviewTime) FROM ProfileView " +
                        " WHERE ViewedProfileWorkPlace = '%s'"+
                        "%s"


		New changes*/
		 "SELECT COUNT(*), AVG(ReviewTime), AVG(ViewerProfileStrength) FROM ProfileView" +     
		    					" WHERE %s",
			 				//" WHERE ViewedProfileId = '%s'"+
		    					//"%s",
		    					
    					
			"SELECT ViewerPosition, COUNT(*) FROM ProfileView" +
							" WHERE ViewedProfileId = '%s'"+
							"%s"+
							" GROUP BY ViewerPosition TOP %d",
					
			"SELECT ViewerWorkPlace, COUNT(*) FROM ProfileView" +
							" WHERE ViewedProfileId = '%s'"+
							"%s"+
							" GROUP BY ViewerWorkPlace TOP %d",
							
			"SELECT COUNT(*), AVG(ReviewTime), AVG(ViewerProfileStrength) FROM ProfileView" +
                        " WHERE ViewedProfilePosition = '%s'"+
                        "%s",
                        
            "SELECT COUNT(*), AVG(ReviewTime), AVG(ViewerProfileStrength) FROM ProfileView " +
                        " WHERE ViewedProfileWorkPlace = '%s'"+
                        "%s"
        /*
	"SELECT ViewedProfilePosition, COUNT(*), AVG(ReviewTime) FROM ProfileView" +
        //		" WHERE ViewStartTime > %d AND ViewStartTime < %d" +
	                " WHERE ViewerWorkPlace = '%s'"+
                        " GROUP BY ViewedProfilePosition TOP %d",
        "SELECT ViewedProfileWorkPlace, COUNT(*), AVG(ReviewTime) FROM ProfileView " +
			" WHERE ViewStartTime > %d AND ViewStartTime < %d" +
        //                " WHERE ViewerWorkPlace = '%s'"+
                        " GROUP BY ViewedProfileWorkPlace TOP %d"
        */
	
	/*
		"SELECT ViewedProfilePosition, COUNT(*), AVG(ReviewTime), AVG(ViewerProfileStrength) FROM ProfileView" +
                        " WHERE ViewStartTime > %d AND ViewStartTime < %d"+
                        " GROUP BY ViewedProfilePosition TOP %d", 
		"SELECT ViewedProfileWorkPlace, COUNT(*), AVG(ReviewTime), AVG(ViewerProfileStrength) FROM ProfileView" +
                        " WHERE ViewStartTime > %d AND ViewStartTime < %d"+
                        " GROUP BY ViewedProfileWorkPlace TOP %d"*/
		//"SELECT ViewerWorkPlace, ViewerPosition, COUNT(*), AVG(ReviewTime) FROM ProfileView" +
                 //   " WHERE ViewStartTime > %d AND ViewStartTime < %d" +
                 //   " GROUP BY ViewerWorkPlace, ViewerPosition TOP %d"

		//,
                //"SELECT ViewerPosition, COUNT(*), AVG(ReviewTime), AVG(ViewedProfileStrength) FROM ProfileView " +
                //    " WHERE ViewStartTime > %d AND ViewStartTime < %d " +
                //    " GROUP BY ViewerPosition TOP %d",
                //"SELECT ViewerWorkPlace, ViewerPosition, ViewedProfileWorkPlace, ViewedProfilePosition, COUNT(*) FROM ProfileView" +
                //    " WHERE ViewStartTime > %d AND ViewStartTime < %d" +
                //    " GROUP BY ViewerWorkPlace, ViewerPosition, ViewedProfileWorkPlace, ViewedProfilePosition TOP %d"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static ProfileViewQueryExecutor getInstance() {
        if (instance == null)
            instance = new ProfileViewQueryExecutor();
        return instance;
    }

    public ProfileViewQueryTask getTask(Properties config) {
        return new ProfileViewQueryTask(config, QUERIES, _dataDir, _testDuration, getCriteria(Constant.MAX_PROFILE_START_TIME,Constant.MIN_PROFILE_START_TIME));
    }
}
