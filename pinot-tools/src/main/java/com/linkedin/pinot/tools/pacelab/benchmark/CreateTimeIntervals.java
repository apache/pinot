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

import java.util.ArrayList;
import java.util.List;

public class CreateTimeIntervals {
    public static void main(String args[])
    {
        long startTime = 1514764801;
        int segmentCount = 40;

        for(int i=0;i<segmentCount;i++)
        {
            String folderName = "Day-" + (i+1);
            long intStartTime= startTime;
            long intEndTime = intStartTime + 24*3600;

            startTime = intEndTime +1;
            Integer segNames[] = new Integer[4];

            for(int j=1;j<=4;j++)
            {
                segNames[j-1]=((j*1000)+(i+1))*10000+2018;
            }

            System.out.println(folderName + "," + intStartTime + "," + intEndTime + "," + segNames[0] + "," + segNames[1] + "," + segNames[2] + "," + segNames[3]);
        }


    }
}
