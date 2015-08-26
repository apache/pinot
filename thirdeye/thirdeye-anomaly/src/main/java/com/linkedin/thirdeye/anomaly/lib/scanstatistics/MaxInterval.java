package com.linkedin.thirdeye.anomaly.lib.scanstatistics;

import org.apache.commons.math3.util.Pair;


/**
 * Created with IntelliJ IDEA.
 * User: jjchen
 * Date: 8/25/15
 * Time: 10:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxInterval {

        public double _maxLikelihood;
        public Pair<Integer, Integer> _interval;

        public MaxInterval(double maxLikelihood, Pair<Integer, Integer> interval) {
            _maxLikelihood = maxLikelihood;
            _interval= interval;
        }

}


