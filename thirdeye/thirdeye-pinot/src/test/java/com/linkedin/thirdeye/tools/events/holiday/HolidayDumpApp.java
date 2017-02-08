package com.linkedin.thirdeye.tools.events.holiday;

import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.CalendarScopes;
import com.google.api.services.calendar.model.*;
import com.google.api.client.auth.oauth2.Credential;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class HolidayDumpApp {

  /**
   * Application name.
   */
  private static final String APPLICATION_NAME = "Google Calendar API Java Quickstart";

  /**
   * Directory to store user credentials for this application.
   */
  private static final java.io.File DATA_STORE_DIR =
      new java.io.File(System.getProperty("user.home"), ".credentials/calendar-java-quickstart");

  /**
   * Global instance of the {@link FileDataStoreFactory}.
   */
  private static FileDataStoreFactory DATA_STORE_FACTORY;

  /**
   * Global instance of the JSON factory.
   */
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /**
   * Global instance of the HTTP transport.
   */
  private static HttpTransport HTTP_TRANSPORT;

  /**
   * Global instance of the scopes required by this quickstart. If modifying these scopes, delete
   * your previously saved credentials at ~/.credentials/calendar-java-quickstart
   */
  private static final List<String> SCOPES = Arrays.asList(CalendarScopes.CALENDAR_READONLY);

  static {
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Creates an authorized Credential object.
   *
   * @return an authorized Credential object.
   *
   * @throws java.io.IOException
   */
  public static Credential authorize() throws IOException {
    // Load client secrets.
    InputStream in = new FileInputStream(new File(
        "/opt/Code/pinot2_0/thirdeye/thirdeye-pinot/src/test/resources/holidays/client-secret.json"));
    GoogleClientSecrets clientSecrets =
        GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

    // Build flow and trigger user authorization request.
    GoogleAuthorizationCodeFlow flow =
        new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
            .setDataStoreFactory(DATA_STORE_FACTORY).setAccessType("offline").build();
    Credential credential =
        new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
    System.out.println("Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
    return credential;
  }

  /**
   * Build and return an authorized Calendar client service.
   *
   * @return an authorized Calendar client service
   *
   * @throws IOException
   */
  public static com.google.api.services.calendar.Calendar getCalendarService() throws IOException {
    Credential credential = authorize();
    return new com.google.api.services.calendar.Calendar.Builder(HTTP_TRANSPORT, JSON_FACTORY,
        credential).setApplicationName(APPLICATION_NAME).build();
  }

  public static void main(String[] args) throws Exception {
    // Build a new authorized API client service.
    // Note: Do not confuse this class with the
    //   com.google.api.services.calendar.model.Calendar class.
    com.google.api.services.calendar.Calendar service = getCalendarService();

    // List the next 10 events from the primary calendar.
    DateTime start0 = new DateTime(System.currentTimeMillis() - 80 * 86400_000);
    Scanner scanner = new Scanner(new File(
        "/opt/Code/pinot2_0/thirdeye/thirdeye-pinot/src/test/resources/holidays/calender-list.csv"));
    Map<String, List<String>> dateWiseHolidayCountryMap = new HashMap<>();

    while (scanner.hasNext()) {
      String[] countryCalendarArr = scanner.nextLine().split(",");
      String countryCode = countryCalendarArr[0];
      String holidayKey = countryCalendarArr[2];
      try {
        Events events = service.events().list(holidayKey).setMaxResults(1000_000).setTimeMin(start0)
            .setSingleEvents(true).execute();
        List<Event> items = events.getItems();
        if (items.size() == 0) {
        } else {
          for (Event event : items) {
            DateTime start = event.getStart().getDateTime();
            if (start == null) {
              start = event.getStart().getDate();
            }
            String key = start + "|" + event.getSummary();
            if (!dateWiseHolidayCountryMap.containsKey(key)) {
              dateWiseHolidayCountryMap.put(key, new ArrayList<>());
            }
            dateWiseHolidayCountryMap.get(key).add(countryCode);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    System.out.println(dateWiseHolidayCountryMap);
    dumpHolidayEventsToDB(dateWiseHolidayCountryMap);
  }

  static SimpleDateFormat formatter =  new SimpleDateFormat("yyyy-MM-dd");

  static void dumpHolidayEventsToDB(Map<String, List<String>> dateWiseHolidayCountryMap)
      throws Exception {
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String persistenceConfigPath = "/opt/Code/thirdeye-configs/prod-configs/persistence.yml";
    File persistenceFile = new File(persistenceConfigPath);
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    DaoProviderUtil.init(persistenceFile);
    EventManager eventManager = DaoProviderUtil.getInstance(EventManagerImpl.class);
    // in case if you want to clean up existing events before
//    for (EventDTO eventDTO : eventManager.findAll()) {
//      eventManager.deleteById(eventDTO.getId());
//    }
    for (Map.Entry<String, List<String>> keyVal : dateWiseHolidayCountryMap.entrySet()) {
      String[] dateHolidayArr = keyVal.getKey().split("\\|");
      String date = dateHolidayArr[0];
      String holidayName = dateHolidayArr[1];

      Date start = formatter.parse(date);
      long startTime = start.getTime();
      long endTime = startTime + TimeUnit.DAYS.toMillis(1);

      List<EventDTO> existingEvents = eventManager
          .findEventsBetweenTimeRangeByName(EventType.HOLIDAY.name(), holidayName, startTime,
              endTime);

      List<String> countries = keyVal.getValue();

      // Add new event only when there are no conflicting (by name and time range) events
      if (existingEvents.size() == 0) {
        EventDTO eventDTO = new EventDTO();
        eventDTO.setName(holidayName);
        Map<String, List<String>> targetDimensionsMap = new HashMap<>();
        targetDimensionsMap.put("countryCode", countries);
        eventDTO.setTargetDimensionMap(targetDimensionsMap);
        eventDTO.setStartTime(startTime);
        eventDTO.setEndTime(endTime);
        eventDTO.setEventType(EventType.HOLIDAY.name());
        System.out.println(eventDTO);
        Long id = eventManager.save(eventDTO);
        System.out.println("created event with id " + id);
      }
    }

    System.out.println("Total events in the table : " + eventManager.findAll().size());
  }

}
