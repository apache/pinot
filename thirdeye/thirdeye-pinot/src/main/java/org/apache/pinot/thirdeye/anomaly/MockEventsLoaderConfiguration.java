/*
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

package org.apache.pinot.thirdeye.anomaly;


import org.apache.pinot.thirdeye.anomaly.events.MockEventsLoader;

import java.util.Collections;
import java.util.List;

/**
 * Configuration container for generating series of mock events
 *
 * @see MockEventsLoader
 */
public class MockEventsLoaderConfiguration {
    public static class EventGeneratorConfig {

        /** EventDTO event type */
        String type;

        /** Distribution type of arrival times distribution */
        String arrivalType = MockEventsLoader.DIST_TYPE_EXPONENTIAL;

        /** Distribution mean of arrival time distribution */
        double arrivalMean;

        /** Distribution type of event duration distribution */
        String durationType = MockEventsLoader.DIST_TYPE_FIXED;

        /** Distribution mean of event duration distribution */
        double durationMean = 86400000;

        /** Distribution RNG seed for deterministic sampling */
        int seed = 0;

        /** List of event name prefixes */
        List<String> namePrefixes = Collections.emptyList();

        /** List of event name suffixes */
        List<String> nameSuffixes = Collections.emptyList();

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

        public List<String> getNamePrefixes() {
            return namePrefixes;
        }

        public void setNamePrefixes(List<String> namePrefixes) {
            this.namePrefixes = namePrefixes;
        }

        public List<String> getNameSuffixes() {
            return nameSuffixes;
        }

        public void setNameSuffixes(List<String> nameSuffixes) {
            this.nameSuffixes = nameSuffixes;
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
