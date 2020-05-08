package org.apache.pinot.thirdeye.anomaly.events;

import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.random.Well19937c;
import org.apache.pinot.thirdeye.anomaly.MockEventsLoaderConfiguration;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class MockEventsLoader implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MockEventsLoader.class);

    private static Comparator<EventDTO> MOCK_EVENT_COMPARATOR = Comparator
            .comparingLong(EventDTO::getStartTime)
            .thenComparingLong(EventDTO::getEndTime)
            .thenComparing(EventDTO::getEventType)
            .thenComparing(EventDTO::getName);

    private static final long START_TIMESTAMP = 1546300800000L; // January 1, 2019 12:00:00 AM GMT
    private static final long END_OFFSET = 31536000000L; // 365d in ms

    public static final String DIST_TYPE_GAUSSIAN = "gaussian";
    public static final String DIST_TYPE_EXPONENTIAL = "exponential";
    public static final String DIST_TYPE_LOGNORMAL = "lognormal";
    public static final String DIST_TYPE_FIXED = "fixed";

    MockEventsLoaderConfiguration configuration;
    EventManager eventDAO;

    public MockEventsLoader(MockEventsLoaderConfiguration configuration, EventManager eventDAO) {
        this.configuration = configuration;
        this.eventDAO = eventDAO;
    }

    @Override
    public void run() {
        final long cutoff = System.currentTimeMillis() + END_OFFSET;

        for (MockEventsLoaderConfiguration.EventGeneratorConfig conf : this.configuration.getGenerators()) {
            LOG.info("Generating '{}' events from {} to {}", conf.getType(), START_TIMESTAMP, cutoff);

            List<EventDTO> generated = generateEvents(conf, cutoff);
            List<EventDTO> existing = this.eventDAO.findEventsBetweenTimeRange(conf.getType(), START_TIMESTAMP, cutoff);

            Set<EventDTO> deduplicated = dedup(generated, existing);
            LOG.info("Generated '{}' events: {} generated, {} pre-existing, {} saved after deduplication",
                    conf.getType(), generated.size(), existing.size(), deduplicated.size());

            deduplicated.forEach(this.eventDAO::save);
        }
    }

    List<EventDTO> generateEvents(MockEventsLoaderConfiguration.EventGeneratorConfig conf, long cutoff) {
        List<EventDTO> generated = new ArrayList<>();

        AbstractRealDistribution arrivalDist = makeDist(conf.getArrivalType(), conf.getArrivalMean(), conf.getSeed());
        AbstractRealDistribution durationDist = makeDist(conf.getDurationType(), conf.getDurationMean(), conf.getSeed());

        EventGenerator generator = new EventGenerator(conf.getType(), arrivalDist, durationDist);

        EventDTO event;
        while ((event = generator.next()).getStartTime() < cutoff) {
            generated.add(event);
        }

        return generated;
    }

    Set<EventDTO> dedup(Collection<EventDTO> generated, Collection<EventDTO> existing) {
        Set<EventDTO> sorted = new TreeSet<>(MOCK_EVENT_COMPARATOR);
        sorted.addAll(generated);
        existing.forEach(sorted::remove);
        return sorted;
    }

    AbstractRealDistribution makeDist(String type, double param, int seed) {
        switch (type.toLowerCase()) {
            case DIST_TYPE_FIXED:
                return new UniformRealDistribution(param, param + 0.001);
            case DIST_TYPE_GAUSSIAN:
                return new NormalDistribution(new Well19937c(seed), param, 1.0d, 1.0E-9D);
            case DIST_TYPE_EXPONENTIAL:
                return new ExponentialDistribution(new Well19937c(seed), param, 1.0E-9D);
            case DIST_TYPE_LOGNORMAL:
                return new LogNormalDistribution(new Well19937c(seed), param, 1.0d, 1.0E-9D);
            default:
                throw new IllegalArgumentException(String.format("Unsupported distribution type '%s'", type));
        }
    }

    static class EventGenerator {
        String type;
        AbstractRealDistribution arrivalDist;
        AbstractRealDistribution durationDist;

        int eventCount = 0;
        long lastTimestamp = START_TIMESTAMP;

        public EventGenerator(String type, AbstractRealDistribution arrivalDist, AbstractRealDistribution durationDist) {
            this.type = type;
            this.arrivalDist = arrivalDist;
            this.durationDist = durationDist;
        }

        public EventDTO next() {
            long arrival = lastTimestamp + (long) this.arrivalDist.sample();
            long duration = (long) this.durationDist.sample();

            this.lastTimestamp = arrival;

            EventDTO event = new EventDTO();
            event.setStartTime(arrival);
            event.setEndTime(arrival + duration);
            event.setName(this.makeName());
            event.setEventType(this.type.toUpperCase());
            event.setTargetDimensionMap(Collections.emptyMap());

            return event;
        }

        String makeName() {
            return String.format("%s-%d", this.type, ++this.eventCount);
        }
    }
}
