package com.linkedin.thirdeye.anomaly.events;

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
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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

  private class HolidayEvent {
    /**
     * The Name.
     */
    String name;
    /**
     * The Event type.
     */
    String eventType;

    /**
     * The Start time.
     */
    long startTime;
    /**
     * The End time.
     */
    long endTime;

    /**
     * Instantiates a new Holiday event.
     *
     * @param name the name
     * @param eventType the event type
     * @param startTime the start time
     * @param endTime the end time
     */
    public HolidayEvent(String name, String eventType, long startTime, long endTime) {
      this.name = name;
      this.eventType = eventType;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    /**
     * Instantiates a new Holiday event.
     *
     * @param eventDTO the event dto
     */
    public HolidayEvent(EventDTO eventDTO) {
      this.name = eventDTO.getName();
      this.eventType = eventDTO.getEventType();
      this.startTime = eventDTO.getStartTime();
      this.endTime = eventDTO.getEndTime();
    }

    /**
     * Gets name.
     *
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name.
     *
     * @param name the name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets start time.
     *
     * @return the start time
     */
    public long getStartTime() {
      return startTime;
    }

    /**
     * Sets start time.
     *
     * @param startTime the start time
     */
    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }

    /**
     * Gets end time.
     *
     * @return the end time
     */
    public long getEndTime() {
      return endTime;
    }

    /**
     * Sets end time.
     *
     * @param endTime the end time
     */
    public void setEndTime(long endTime) {
      this.endTime = endTime;
    }

    /**
     * Gets event type.
     *
     * @return the event type
     */
    public String getEventType() {
      return eventType;
    }

    /**
     * Sets event type.
     *
     * @param eventType the event type
     */
    public void setEventType(String eventType) {
      this.eventType = eventType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getEventType(), getStartTime(), getEndTime());
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof HolidayEvent)) {
        return false;
      }
      HolidayEvent holidayEvent = (HolidayEvent) obj;
      return Objects.equals(getName(), holidayEvent.getName()) && Objects.equals(getStartTime(),
          holidayEvent.getStartTime()) && Objects.equals(getEndTime(), holidayEvent.getEndTime()) && Objects.equals(
          getEventType(), holidayEvent.getEventType());
    }
  }

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

  /** Api private key path */
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
      LOG.error("Can't create http transport with google api.", e);
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

    long start = System.currentTimeMillis();
    long end = start + 2592000000L; // to get holidays within a month

    List<Event> newHolidays = getAllHolidays(start, end);

    // A map from new holiday to a set of country codes that has the holiday
    Map<HolidayEvent, Set<String>> newHolidayEventToCountryCodes = new HashMap<>();

    // Group by holidays and put the country codes in the corresponding set
    for (Event holiday : newHolidays) {
      HolidayEvent holidayEvent =
          new HolidayEvent(holiday.getSummary(), EventType.HOLIDAY.toString(), holiday.getStart().getDate().getValue(),
              holiday.getEnd().getDate().getValue());

      if (!newHolidayEventToCountryCodes.containsKey(holidayEvent)) {
        newHolidayEventToCountryCodes.put(holidayEvent, new HashSet<String>());
      }
      newHolidayEventToCountryCodes.get(holidayEvent).add(getCountryCode(holiday));
    }

    // Get the existing holidays in the time range
    List<EventDTO> existingEvents = eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.toString(), start, end);
    Map<HolidayEvent, EventDTO> existingHolidayEventToEventDTO = new HashMap<>();
    Map<HolidayEvent, Set<String>> existingHolidayEventToCountryCodes = new HashMap<>();

    // Map existing event DTO to the tuple of HolidayEvent and country code set
    for (EventDTO existingEventDTO : existingEvents) {
      HolidayEvent holidayEvent = new HolidayEvent(existingEventDTO);
      if (!existingHolidayEventToCountryCodes.containsKey(holidayEvent)) {
        existingHolidayEventToCountryCodes.put(holidayEvent, new HashSet<String>());
      }
      if (existingEventDTO.getTargetDimensionMap() != null
          && existingEventDTO.getTargetDimensionMap().get("countryCode") != null) {
        existingHolidayEventToCountryCodes.get(holidayEvent)
            .addAll(existingEventDTO.getTargetDimensionMap().get("countryCode"));
      }
      existingHolidayEventToEventDTO.put(holidayEvent, existingEventDTO);
    }

    for (Map.Entry<HolidayEvent, Set<String>> entry : newHolidayEventToCountryCodes.entrySet()) {
      HolidayEvent newHolidayEvent = entry.getKey();
      Set<String> newCountryCodes = entry.getValue();

      if (existingHolidayEventToCountryCodes.containsKey(newHolidayEvent)) {
        if (existingHolidayEventToCountryCodes.get(newHolidayEvent).addAll(newCountryCodes)) {
          // If the holiday is already exist and the country codes are different, merge with new country codes and update the database
          Map<String, List<String>> targetDimensionMap = new HashMap<>();
          targetDimensionMap.put("countryCode", new ArrayList<>(newCountryCodes));

          EventDTO eventDTO = existingHolidayEventToEventDTO.get(newHolidayEvent);
          eventDTO.setTargetDimensionMap(targetDimensionMap);
          eventDAO.update(eventDTO);
        }
      } else {
        // If the holiday is not exist, insert it into the database
        EventDTO eventDTO = new EventDTO();
        eventDTO.setName(entry.getKey().getName());
        eventDTO.setEventType(entry.getKey().getEventType());
        eventDTO.setStartTime(entry.getKey().getStartTime());
        eventDTO.setEndTime(entry.getKey().getEndTime());

        Map<String, List<String>> targetDimensionMap = new HashMap<>();
        targetDimensionMap.put("countryCode", new ArrayList<>(entry.getValue()));
        eventDTO.setTargetDimensionMap(targetDimensionMap);
        eventDAO.save(eventDTO);
      }
    }
  }

  private String getCountryCode(Event holiday) {
    String countryName = holiday.getCreator().getDisplayName().substring(12);
    for (Locale locale : Locale.getAvailableLocales()) {
      if (locale.getDisplayCountry().equals(countryName)) {
        return locale.getCountry();
      }
    }
    return null;
  }

  /**
   * Fetch all holidays from Google Calendar API.
   */
  private List<Event> getAllHolidays(Long start, Long end) {
    List<Event> events = new ArrayList<>();
    for (String calendar : calendarList) {
      try {
        events.addAll(this.getCalendarEvents(calendar, start, end));
      } catch (Exception e) {
        LOG.error("{} Fetch holiday events failed.", calendar);
      }
    }
    return events;
  }

  private List<Event> getCalendarEvents(String Calendar_id, Long start, Long end) throws Exception {
    GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(keyPath)).createScoped(SCOPES);
    Calendar service =
        new Calendar.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName("thirdeye").build();
    Events events =
        service.events().list(Calendar_id).setTimeMin(new DateTime(start)).setTimeMax(new DateTime(end)).execute();
    return events.getItems();
  }
}
