package org.apache.pinot.thirdeye.anomaly;


import org.apache.pinot.thirdeye.anomaly.events.MockEventsLoader;

import java.util.Collections;
import java.util.List;

public class MockEventsLoaderConfiguration {
    public static class EventGeneratorConfig {
        String type;
        String arrivalType = MockEventsLoader.DIST_TYPE_EXPONENTIAL;
        double arrivalMean;
        String durationType = MockEventsLoader.DIST_TYPE_FIXED;
        double durationMean = 86400000;
        int seed = 0;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getArrivalType() {
            return arrivalType;
        }

        public void setArrivalType(String arrivalType) {
            this.arrivalType = arrivalType;
        }

        public double getArrivalMean() {
            return arrivalMean;
        }

        public void setArrivalMean(double arrivalMean) {
            this.arrivalMean = arrivalMean;
        }

        public String getDurationType() {
            return durationType;
        }

        public void setDurationType(String durationType) {
            this.durationType = durationType;
        }

        public double getDurationMean() {
            return durationMean;
        }

        public void setDurationMean(double durationMean) {
            this.durationMean = durationMean;
        }

        public int getSeed() {
            return seed;
        }

        public void setSeed(int seed) {
            this.seed = seed;
        }
    }

    List<EventGeneratorConfig> generators = Collections.emptyList();

    public List<EventGeneratorConfig> getGenerators() {
        return generators;
    }

    public void setGenerators(List<EventGeneratorConfig> generators) {
        this.generators = generators;
    }
}
