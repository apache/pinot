package com.linkedin.thirdeye.anomaly.events;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventDateTime;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest(HolidayEventsLoader.class)
public class HolidayEventsLoaderTest {

  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  @BeforeMethod
   public void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    testDAOProvider.cleanup();
  }

  @Test
  public void loadsHolidayEventsTest() throws Exception {
    ThirdEyeAnomalyConfiguration thirdEyeAnomalyConfiguration = new ThirdEyeAnomalyConfiguration();
    thirdEyeAnomalyConfiguration.setHolidayRange(2592000000L);
    HolidayEventsLoader spy = PowerMockito.spy(new HolidayEventsLoader(thirdEyeAnomalyConfiguration));

    Event firstEvent = new Event();
    firstEvent.setStart(new EventDateTime().setDateTime(new DateTime(100)));
    firstEvent.setEnd(new EventDateTime().setDateTime(new DateTime(200)));
    firstEvent.setSummary("Some festival");
    firstEvent.setCreator(new Event.Creator().setDisplayName("Holidays in United States"));

    Event secondEvent = new Event();
    secondEvent.setStart(new EventDateTime().setDateTime(new DateTime(100)));
    secondEvent.setEnd(new EventDateTime().setDateTime(new DateTime(200)));
    secondEvent.setSummary("Some festival");
    secondEvent.setCreator(new Event.Creator().setDisplayName("Holidays in United States"));

    Event thirdEvent = new Event();
    thirdEvent.setStart(new EventDateTime().setDateTime(new DateTime(200)));
    thirdEvent.setEnd(new EventDateTime().setDateTime(new DateTime(300)));
    thirdEvent.setSummary("Another festival");
    thirdEvent.setCreator(new Event.Creator().setDisplayName("Holidays in China"));

    when(spy, method(HolidayEventsLoader.class, "getAllHolidays", long.class, long.class))
        .withArguments(long.class, long.class).thenReturn(Arrays.asList(firstEvent, secondEvent, thirdEvent));
    spy.run();
  }


}
