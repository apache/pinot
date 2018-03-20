package com.linkedin.thirdeye.rootcause.impl;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.CalendarScopes;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.Events;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The type Holiday events loader, which loads the holiday events from Google Calendar periodically.
 */
public class HolidayEventsLoader implements Runnable {

  /**
   * Instantiates a new Holiday events loader.
   *
   * @param configuration the configuration
   */
  public HolidayEventsLoader(ThirdEyeAnomalyConfiguration configuration) {
    calendarList = configuration.getCalendars();
    keyPath = configuration.getRootDir() + configuration.getKeyPath();
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  /** List of google holiday calendar ids */
  private static List<String> calendarList;
  private static String keyPath;
  private ScheduledExecutorService scheduledExecutorService;
  private TimeGranularity runFrequency = new TimeGranularity(7, TimeUnit.DAYS);

  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsLoader.class);

  /** Global instance of the HTTP transport. */
  private static HttpTransport HTTP_TRANSPORT;

  /** Global instance of the JSON factory. */
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /** Global instance of the scopes. */
  private static final Set<String> SCOPES = Collections.singleton(CalendarScopes.CALENDAR_READONLY);

  static {
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
    } catch (Exception e) {
      LOG.error("Can't create http transport with google api.");
    }
  }

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  /**
   * Start.
   */
  public void start() {
    scheduledExecutorService.scheduleAtFixedRate(this, 0, runFrequency.getSize(), runFrequency.getUnit());
  }

  /**
   * Shutdown.
   */
  public void shutdown() {
    scheduledExecutorService.shutdown();
  }

  /**
   * Fetch holidays and save to ThirdEye database.
   */
  public void run() {
    EventManager eventDAO = DAO_REGISTRY.getEventDAO();

    List<Event> holidays = getAllHolidays();
    Set<EventDTO> newEvents = new HashSet<>();

    for (Event holiday : holidays) {
      EventDTO eventDTO = new EventDTO();

      eventDTO.setStartTime(holiday.getStart().getDate().getValue());
      eventDTO.setEndTime(holiday.getEnd().getDate().getValue());
      eventDTO.setName(holiday.getSummary());
      eventDTO.setEventType(EventType.HOLIDAY.toString());

      newEvents.add(eventDTO);
    }
    Set<EventDTO> existingEvents = new HashSet<>(eventDAO.findAll());

    // remove existing holidays
    newEvents.removeAll(existingEvents);

    // add new holidays
    for (EventDTO newEvent : newEvents) {
      eventDAO.save(newEvent);
    }
  }

  /**
   * Fetch all holidays from Google Calendar API.
   */
  private List<Event> getAllHolidays() {
    List<Event> events = new ArrayList<>();
    for (String calendar : calendarList) {
      try {
        events.addAll(this.getCalendarEvents(calendar));
      } catch (Exception e) {
        // left blank
      }
    }
    return events;
  }

  private List<Event> getCalendarEvents(String Calendar_id) throws Exception {

    GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(keyPath)).createScoped(SCOPES);
    Calendar service =
        new Calendar.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName("thirdeye").build();
    Events events = service.events().list(Calendar_id).setTimeMin(new DateTime(System.currentTimeMillis())).execute();
    return events.getItems();
  }
}
