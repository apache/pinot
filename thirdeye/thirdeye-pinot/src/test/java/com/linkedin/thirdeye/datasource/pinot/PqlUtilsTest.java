package com.linkedin.thirdeye.datasource.pinot;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.pinot.PqlUtils;

public class PqlUtilsTest {

  @Test(dataProvider = "betweenClauseArgs")
  public void getBetweenClause(DateTime start, DateTime end, TimeSpec timeFieldSpec,
      String expected) throws ExecutionException {
    String collection = "collection";
    DatasetConfigDTO datasetConfig = new DatasetConfigDTO();
    LoadingCache<String, DatasetConfigDTO> mockDatasetConfigCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockDatasetConfigCache.get(collection)).thenReturn(datasetConfig);
    ThirdEyeCacheRegistry.getInstance().registerDatasetConfigCache(mockDatasetConfigCache);

    String betweenClause = PqlUtils.getBetweenClause(start, end, timeFieldSpec, "collection");
    Assert.assertEquals(betweenClause, expected);
  }

  @DataProvider(name = "betweenClauseArgs")
  public Object[][] betweenClauseArgs() {
    return new Object[][] {
        // equal start+end, no format
        getBetweenClauseTestArgs("2016-01-01T00:00:00.000+00:00", "2016-01-01T00:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, null, " timeColumn = 201612"),
        // "" with date range
        getBetweenClauseTestArgs("2016-01-01T00:00:00.000+00:00", "2016-01-02T00:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, null, " timeColumn >= 201612 AND timeColumn < 201624"),
        // equal start+end, with format
        getBetweenClauseTestArgs("2016-01-01T08:00:00.000+00:00", "2016-01-01T08:00:00.000+00:00",
            "timeColumn", 1, TimeUnit.DAYS, "yyyyMMdd", " timeColumn = 20160101"),
        // "" with date range
        getBetweenClauseTestArgs("2016-01-01T08:00:00.000+00:00", "2016-01-02T08:00:00.000+00:00",
            "timeColumn", 1, TimeUnit.DAYS, "yyyyMMdd",
            " timeColumn >= 20160101 AND timeColumn < 20160102"),
        // Incorrectly aligned date ranges, no format
        getBetweenClauseTestArgs("2016-01-01T01:00:00.000+00:00", "2016-01-01T23:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, null, " timeColumn >= 201613 AND timeColumn < 201623"),
        // Incorrectly aligned date ranges, with format
        getBetweenClauseTestArgs("2016-01-01T01:00:00.000+00:00", "2016-01-01T23:00:00.000+00:00",
            "timeColumn", 2, TimeUnit.HOURS, "yyyyMMddHH",
            " timeColumn >= 2016010101 AND timeColumn < 2016010123")
    };
  }

  private Object[] getBetweenClauseTestArgs(String startISO, String endISO, String timeColumn,
      int timeGranularitySize, TimeUnit timeGranularityUnit, String timeSpecFormat,
      String expected) {
    return new Object[] {
        new DateTime(startISO, DateTimeZone.UTC), new DateTime(endISO, DateTimeZone.UTC),
        new TimeSpec(timeColumn, new TimeGranularity(timeGranularitySize, timeGranularityUnit),
            timeSpecFormat),
        expected
    };
  }
}
