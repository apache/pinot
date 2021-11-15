package org.apache.pinot.tools.admin.command;

import org.apache.pinot.tools.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;

public class TestQuickStartCommand {

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "^No QuickStart type provided. Valid types are: \\[.*\\]$")
    public void testNoArg() throws Exception {
        QuickStartCommand quickStartCommand = new QuickStartCommand();
        quickStartCommand.execute();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "^Unsupported QuickStart type: foo. Valid types are: \\[.*\\]$")
    public void testInvalidQuickStart() throws Exception {
        QuickStartCommand quickStartCommand = new QuickStartCommand();
        quickStartCommand.setType("foo");
        quickStartCommand.execute();
    }

    @Test
    public void testMatchStringToCommand() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Assert.assertEquals(quickStartClassFor("OFFLINE"), Quickstart.class);
        Assert.assertEquals(quickStartClassFor("BATCH"), Quickstart.class);

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

        Assert.assertEquals(quickStartClassFor("OFFLINE_COMPLEX_TYPE"), OfflineComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("OFFLINE-COMPLEX-TYPE"), OfflineComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("BATCH_COMPLEX_TYPE"), OfflineComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("BATCH-COMPLEX-TYPE"), OfflineComplexTypeHandlingQuickStart.class);

        Assert.assertEquals(quickStartClassFor("REALTIME_COMPLEX_TYPE"), RealtimeComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("REALTIME-COMPLEX-TYPE"), RealtimeComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("STREAM_COMPLEX_TYPE"), RealtimeComplexTypeHandlingQuickStart.class);
        Assert.assertEquals(quickStartClassFor("STREAM-COMPLEX-TYPE"), RealtimeComplexTypeHandlingQuickStart.class);
    }

    private Class<? extends QuickStartBase> quickStartClassFor(String offline) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return QuickStartCommand.selectQuickStart(offline).getClass();
    }
}
