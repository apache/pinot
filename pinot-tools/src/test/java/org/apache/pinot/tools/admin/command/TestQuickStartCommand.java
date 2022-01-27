/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.admin.command;

import java.lang.reflect.InvocationTargetException;
import org.apache.pinot.tools.BatchQuickstartWithMinion;
import org.apache.pinot.tools.EmptyQuickstart;
import org.apache.pinot.tools.HybridQuickstart;
import org.apache.pinot.tools.JoinQuickStart;
import org.apache.pinot.tools.JsonIndexQuickStart;
import org.apache.pinot.tools.OfflineComplexTypeHandlingQuickStart;
import org.apache.pinot.tools.QuickStartBase;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.RealtimeComplexTypeHandlingQuickStart;
import org.apache.pinot.tools.RealtimeJsonIndexQuickStart;
import org.apache.pinot.tools.RealtimeQuickStart;
import org.apache.pinot.tools.RealtimeQuickStartWithMinion;
import org.apache.pinot.tools.UpsertJsonQuickStart;
import org.apache.pinot.tools.UpsertQuickStart;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestQuickStartCommand {

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "^No QuickStart type provided. Valid types are: \\[.*\\]$")
    public void testNoArg() throws Exception {
        QuickStartCommand quickStartCommand = new QuickStartCommand();
        quickStartCommand.execute();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "^Unsupported QuickStart type: foo. Valid types are: \\[.*\\]$")
    public void testInvalidQuickStart() throws Exception {
        QuickStartCommand quickStartCommand = new QuickStartCommand();
        quickStartCommand.setType("foo");
        quickStartCommand.execute();
    }

    @Test
    public void testMatchStringToCommand()
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Assert.assertEquals(quickStartClassFor("OFFLINE"), Quickstart.class);
        Assert.assertEquals(quickStartClassFor("offline"), Quickstart.class);
        Assert.assertEquals(quickStartClassFor("BATCH"), Quickstart.class);

        Assert.assertEquals(quickStartClassFor("EMPTY"), EmptyQuickstart.class);
        Assert.assertEquals(quickStartClassFor("DEFAULT"), EmptyQuickstart.class);

        Assert.assertEquals(quickStartClassFor("OFFLINE_MINION"), BatchQuickstartWithMinion.class);
        Assert.assertEquals(quickStartClassFor("BATCH_MINION"), BatchQuickstartWithMinion.class);
        Assert.assertEquals(quickStartClassFor("OFFLINE-MINION"), BatchQuickstartWithMinion.class);
        Assert.assertEquals(quickStartClassFor("BATCH-MINION"), BatchQuickstartWithMinion.class);

        Assert.assertEquals(quickStartClassFor("REALTIME_MINION"), RealtimeQuickStartWithMinion.class);
        Assert.assertEquals(quickStartClassFor("REALTIME-MINION"), RealtimeQuickStartWithMinion.class);

        Assert.assertEquals(quickStartClassFor("REALTIME"), RealtimeQuickStart.class);
        Assert.assertEquals(quickStartClassFor("REALTIME"), RealtimeQuickStart.class);

        Assert.assertEquals(quickStartClassFor("HYBRID"), HybridQuickstart.class);

        Assert.assertEquals(quickStartClassFor("JOIN"), JoinQuickStart.class);

        Assert.assertEquals(quickStartClassFor("UPSERT"), UpsertQuickStart.class);

        Assert.assertEquals(quickStartClassFor("OFFLINE_JSON_INDEX"), JsonIndexQuickStart.class);
        Assert.assertEquals(quickStartClassFor("OFFLINE-JSON-INDEX"), JsonIndexQuickStart.class);
        Assert.assertEquals(quickStartClassFor("BATCH_JSON_INDEX"), JsonIndexQuickStart.class);
        Assert.assertEquals(quickStartClassFor("BATCH-JSON-INDEX"), JsonIndexQuickStart.class);

        Assert.assertEquals(quickStartClassFor("REALTIME_JSON_INDEX"), RealtimeJsonIndexQuickStart.class);
        Assert.assertEquals(quickStartClassFor("REALTIME-JSON-INDEX"), RealtimeJsonIndexQuickStart.class);
        Assert.assertEquals(quickStartClassFor("STREAM_JSON_INDEX"), RealtimeJsonIndexQuickStart.class);
        Assert.assertEquals(quickStartClassFor("STREAM-JSON-INDEX"), RealtimeJsonIndexQuickStart.class);

        Assert.assertEquals(quickStartClassFor("UPSERT_JSON_INDEX"), UpsertJsonQuickStart.class);
        Assert.assertEquals(quickStartClassFor("UPSERT-JSON-INDEX"), UpsertJsonQuickStart.class);

        Assert.assertEquals(quickStartClassFor("OFFLINE_COMPLEX_TYPE"),
                OfflineComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("OFFLINE-COMPLEX-TYPE"),
                OfflineComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("BATCH_COMPLEX_TYPE"),
                OfflineComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("BATCH-COMPLEX-TYPE"),
                OfflineComplexTypeHandlingQuickStart.class);

        Assert.assertEquals(quickStartClassFor("REALTIME_COMPLEX_TYPE"),
                RealtimeComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("REALTIME-COMPLEX-TYPE"),
                RealtimeComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("STREAM_COMPLEX_TYPE"),
                RealtimeComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("STREAM-COMPLEX-TYPE"),
                RealtimeComplexTypeHandlingQuickStart.class);
    }

    private Class<? extends QuickStartBase> quickStartClassFor(String offline)
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return QuickStartCommand.selectQuickStart(offline).getClass();
    }
}
