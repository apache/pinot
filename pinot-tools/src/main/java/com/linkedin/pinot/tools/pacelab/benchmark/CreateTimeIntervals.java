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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class CreateTimeIntervals {
    public static void main(String args[])
    {
        long startTime = 1546300800;
        int segmentCount = 120;
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat sdf2 = new SimpleDateFormat("ddMMyyyy");
        sdf2.setTimeZone(TimeZone.getTimeZone("GMT"));

        for(int i=0;i<segmentCount;i++)
        {
            //String folderName = "Day-" + (i+1);
            Date date = new Date(startTime*1000);
            String folderName = "Date-"+ sdf.format(date);

            long intStartTime= startTime;
            long intEndTime = intStartTime + 24*3600;

            startTime = intEndTime;
            Integer segNames[] = new Integer[5];

            int dateDigit = Integer.parseInt(sdf2.format(date));
            for(int j=1;j<=5;j++)
            {
                segNames[j-1]=j*100000000 + dateDigit;
            }

            System.out.println(folderName + "," + intStartTime + "," + intEndTime + "," + segNames[0] + "," + segNames[1] + "," + segNames[2] + "," + segNames[3] + "," + segNames[4]);
        }


    }
}
