package com.linkedin.thirdeye.anomaly.events;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.CalendarScopes;
import com.google.api.services.calendar.model.Event;
import com.linkedin.thirdeye.anomaly.HolidayEventsLoaderConfiguration;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
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

  static class HolidayEvent {
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
    HolidayEvent(String name, String eventType, long startTime, long endTime) {
      this.name = name;
      this.eventType = eventType;
      this.startTime = startTime;
      this.endTime = endTime;
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
   * @param holidayEventsLoaderConfiguration the configuration
   * @param calendarApiKeyPath the calendar api key path
   * @param eventDAO the event dao
   */
  public HolidayEventsLoader(HolidayEventsLoaderConfiguration holidayEventsLoaderConfiguration,
      String calendarApiKeyPath, EventManager eventDAO) {
    this.holidayLoadRange = holidayEventsLoaderConfiguration.getHolidayLoadRange();
    this.calendarList = holidayEventsLoaderConfiguration.getCalendars();
    this.keyPath = calendarApiKeyPath;
    this.eventDAO = eventDAO;
    this.runFrequency = new TimeGranularity(holidayEventsLoaderConfiguration.getRunFrequency(), TimeUnit.DAYS);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  /** List of google holiday calendar ids */
  private List<String> calendarList;

  /** Calendar Api private key path */
  private String keyPath;
  private ScheduledExecutorService scheduledExecutorService;
  private TimeGranularity runFrequency;

  /** Time range to calculate the upper bound for an holiday's start time. In milliseconds */
  private long holidayLoadRange;

  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsLoader.class);

  /** Global instance of the HTTP transport. */
  private static HttpTransport HTTP_TRANSPORT;

  /** Global instance of the JSON factory. */
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /** Global instance of the scopes. */
  private static final Set<String> SCOPES = Collections.singleton(CalendarScopes.CALENDAR_READONLY);

  private static final String NO_COUNTRY_CODE = "no country code";

  static {
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
    } catch (Exception e) {
      LOG.error("Can't create http transport with google api.", e);
    }
  }

  private final EventManager eventDAO;

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
    long start = System.currentTimeMillis();
    long end = start + holidayLoadRange;

    loadHolidays(start, end);
  }

  public void loadHolidays(long start, long end) {
    List<Event> newHolidays = null;
    try {
      newHolidays = getAllHolidays(start, end);
    } catch (Exception e) {
      LOG.error("Fetch holidays failed. Aborting.");
      return;
    }
    Map<HolidayEvent, Set<String>> newHolidayEventToCountryCodes = aggregateCountryCodesGroupByHolidays(newHolidays);

    Map<String, List<EventDTO>> holidayNameToHolidayEvent = getHolidayNameToEventDtoMap(newHolidayEventToCountryCodes);

    // Get the existing holidays within the time range from the database
    List<EventDTO> existingEvents = eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.toString(), start, end);

    mergeWithExistingHolidays(holidayNameToHolidayEvent, existingEvents);
  }

  private Map<HolidayEvent, Set<String>> aggregateCountryCodesGroupByHolidays(List<Event> newHolidays) {
    // A map from new holiday to a set of country codes that has the holiday
    Map<HolidayEvent, Set<String>> newHolidayEventToCountryCodes = new HashMap<>();

    // Convert Google Event Type to holiday events and aggregates the country code list
    for (Event holiday : newHolidays) {
      HolidayEvent holidayEvent =
          new HolidayEvent(holiday.getSummary(), EventType.HOLIDAY.toString(), holiday.getStart().getDate().getValue(),
              holiday.getEnd().getDate().getValue());

      if (!newHolidayEventToCountryCodes.containsKey(holidayEvent)) {
        newHolidayEventToCountryCodes.put(holidayEvent, new HashSet<String>());
      }
      String countryCode = getCountryCode(holiday);
      if (!countryCode.equals(NO_COUNTRY_CODE)) {
        newHolidayEventToCountryCodes.get(holidayEvent).add(countryCode);
      }
    }
    return newHolidayEventToCountryCodes;
  }

  Map<String, List<EventDTO>> getHolidayNameToEventDtoMap(
      Map<HolidayEvent, Set<String>> newHolidayEventToCountryCodes) {
    Map<String, List<EventDTO>> holidayNameToHolidayEvent = new HashMap<>();

    // Convert Holiday Events to EventDTOs.
    for (Map.Entry<HolidayEvent, Set<String>> entry : newHolidayEventToCountryCodes.entrySet()) {
      HolidayEvent newHolidayEvent = entry.getKey();
      Set<String> newCountryCodes = entry.getValue();
      String holidayName = newHolidayEvent.getName();

      EventDTO eventDTO = new EventDTO();
      eventDTO.setName(holidayName);
      eventDTO.setEventType(newHolidayEvent.getEventType());
      eventDTO.setStartTime(newHolidayEvent.getStartTime());
      eventDTO.setEndTime(newHolidayEvent.getEndTime());

      Map<String, List<String>> targetDimensionMap = new HashMap<>();
      targetDimensionMap.put("countryCode", new ArrayList<>(newCountryCodes));
      eventDTO.setTargetDimensionMap(targetDimensionMap);

      if (!holidayNameToHolidayEvent.containsKey(holidayName)) {
        holidayNameToHolidayEvent.put(holidayName, new ArrayList<EventDTO>());
      }
      holidayNameToHolidayEvent.get(holidayName).add(eventDTO);
    }
    return holidayNameToHolidayEvent;
  }

  void mergeWithExistingHolidays(Map<String, List<EventDTO>> holidayNameToHolidayEvent, List<EventDTO> existingEvents) {
    for (EventDTO existingEvent : existingEvents) {
      String holidayName = existingEvent.getName();
      if (!holidayNameToHolidayEvent.containsKey(holidayName)) {
        // If a event disappears, delete the event
        eventDAO.delete(existingEvent);
      } else {
        // If an existing event shows up again, overwrite with new time and country code.
        List<EventDTO> eventList = holidayNameToHolidayEvent.get(holidayName);
        EventDTO newEvent = eventList.remove(eventList.size() - 1);

        existingEvent.setStartTime(newEvent.getStartTime());
        existingEvent.setEndTime(newEvent.getEndTime());
        existingEvent.setTargetDimensionMap(newEvent.getTargetDimensionMap());
        eventDAO.update(existingEvent);

        if (eventList.isEmpty()) {
          holidayNameToHolidayEvent.remove(holidayName);
        }
      }
    }

    // Add all remaining new events into the database
    for (List<EventDTO> eventDTOList : holidayNameToHolidayEvent.values()) {
      for (EventDTO eventDTO : eventDTOList) {
        eventDAO.save(eventDTO);
      }
    }
  }

  private String getCountryCode(Event holiday) {
    String calendarName = holiday.getCreator().getDisplayName();
    if (calendarName != null && calendarName.length() > 12) {
      String countryName = calendarName.substring(12);
      for (Locale locale : Locale.getAvailableLocales()) {
        if (locale.getDisplayCountry().equals(countryName)) {
          return locale.getCountry();
        }
      }
    }
    return NO_COUNTRY_CODE;
  }

  /**
   * Fetch holidays from all calendars in Google Calendar API
   *
   * @param start Lower bound (inclusive) for an holiday's end time to filter by.
   * @param end Upper bound (exclusive) for an holiday's start time to filter by.
   */
  private List<Event> getAllHolidays(long start, long end) throws Exception {
    List<Event> events = new ArrayList<>();
    for (String calendar : calendarList) {
      try {
        events.addAll(this.getCalendarEvents(calendar, start, end));
      } catch (GoogleJsonResponseException e) {
        LOG.warn("Fetch holiday events failed in calendar {}. {}", calendar, e.getDetails());
      }
    }
    return events;
  }

  private List<Event> getCalendarEvents(String Calendar_id, long start, long end) throws Exception {
    GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream(keyPath)).createScoped(SCOPES);
    Calendar service =
        new Calendar.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName("thirdeye").build();
    return service.events()
        .list(Calendar_id)
        .setTimeMin(new DateTime(start))
        .setTimeMax(new DateTime(end))
        .execute()
        .getItems();
  }
}
