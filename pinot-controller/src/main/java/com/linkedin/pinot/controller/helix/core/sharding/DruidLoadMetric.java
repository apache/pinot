/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.restlet.resources.ServerSegmentInfo;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.ServerPerfMetricsReader;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.helix.model.IdealState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class DruidLoadMetric implements ServerLoadMetric {
    private static final HttpConnectionManager _connectionManager = new MultiThreadedHttpConnectionManager();
    private static final Executor _executor = Executors.newFixedThreadPool(1);

    // RM: half life definition: ln(2) / lambda (time difference that will make the joint cost go down by half)
    private static final double HALF_LIFE = 24.0; // cost function half-life in hours
    static final double LAMBDA = Math.log(2) / HALF_LIFE;
    static final double INV_LAMBDA_SQUARE = 1 / (LAMBDA * LAMBDA);

    private static final double MILLIS_IN_HOUR = 3600000.0; // RM: 3,600,000 ms in an hour
    private static final double MILLIS_FACTOR = MILLIS_IN_HOUR / LAMBDA;

    public static double computeJointSegmentsCost(Double segmentAStartTime,Double segmentAEndTime,String segmentATableName,
                                                  Double segmentBStartTime,Double segmentBEndTime,String segmentBTableName)
    {

        final double t0 = segmentAStartTime*1000;
        final double t1 = (segmentAEndTime*1000 - t0) / MILLIS_FACTOR;
        final double start = (segmentBStartTime*1000 - t0) / MILLIS_FACTOR;
        final double end = (segmentBEndTime*1000 - t0) / MILLIS_FACTOR;

        // constant cost-multiplier for segments of the same datsource
        // Robin: A data source is the Druid equivalent of a database table.
        // Hence to give more cost for segment of same table, we have a constant cost multiplier.
        final double multiplier = segmentATableName.equals(segmentBTableName) ? 2.0 : 1.0;

        return INV_LAMBDA_SQUARE * intervalCost(t1, start, end) * multiplier;
    }

    /**
     * Computes the joint cost of two intervals X = [x_0 = 0, x_1) and Y = [y_0, y_1)
     *
     * cost(X, Y) = \int_{x_0}^{x_1} \int_{y_0}^{y_1} e^{-\lambda |x-y|}dxdy $$
     *
     * lambda = 1 in this particular implementation
     *
     * Other values of lambda can be calculated by multiplying inputs by lambda
     * and multiplying the result by 1 / lambda ^ 2
     *
     * Interval start and end are all relative to x_0.
     * Therefore this function assumes x_0 = 0, x1 >= 0, and y1 > y0
     */
    public static double intervalCost(double x1, double y0, double y1)
    {
        if (x1 == 0 || y1 == y0) {
            return 0;
        }

        // cost(X, Y) = cost(Y, X), so we swap X and Y to
        // have x_0 <= y_0 and simplify the calculations below
        if (y0 < 0) {
            // swap X and Y
            double tmp = x1;
            x1 = y1 - y0;
            y1 = tmp - y0;
            y0 = -y0;
        }

        // since x_0 <= y_0, Y must overlap X if y_0 < x_1
        if (y0 < x1) {
            /**
             * We have two possible cases of overlap:
             *
             * X  = [ A )[ B )[ C )   or  [ A )[ B )
             * Y  =      [   )                 [   )[ C )
             *
             * A is empty if y0 = 0
             * C is empty if y1 = x1
             *
             * cost(X, Y) = cost(A, Y) + cost(B, C) + cost(B, B)
             * // RM: cost (Y=B,B union C) = cost(B,C) + cost(B,B), done to satisfy additive property 3 of cost funct
             * cost(A, Y) and cost(B, C) can be calculated using the non-overlapping case,
             * which reduces the overlapping case to computing
             *
             * cost(B, B) = \int_0^{\beta} \int_{0}^{\beta} e^{-|x-y|}dxdy
             *            = 2 \cdot (\beta + e^{-\beta} - 1)
             *
             *            where \beta is the length of interval B
             *
             */
            final double beta;  // b1 - y0, length of interval B
            final double gamma; // c1 - y0, length of interval C
            if (y1 <= x1) {
                beta = y1 - y0;
                gamma = x1 - y0;
            } else {
                beta = x1 - y0;
                gamma = y1 - y0;
            }
            //noinspection SuspiciousNameCombination
            return intervalCost(y0, y0, y1) + // cost(A, Y)
                    intervalCost(beta, beta, gamma) + // cost(B, C)
                    2 * (beta + Math.exp(-beta) - 1); // cost(B, B)
        } else {
            //RM: using Math instead of FastMath used by Linkedin because use of math suggested in review comments.
            /**
             * In the case where there is no overlap:
             *
             * Given that x_0 <= y_0,
             * then x <= y must be true for all x in [x_0, x_1] and y in [y_0, y_1).
             *
             * therefore,
             *
             * cost(X, Y) = (e^{-y1} - e^{-y0}) - (e^{x1 - y1} - e^{x1 - y0})
             *
             * Note, this expression could be further reduced by factoring out (e^{x1} - 1),
             * but we prefer to keep the smaller values x1 - y0 and x1 - y1 in the exponent
             * to avoid numerical overflow caused by calculating e^{x1}
             */
            final double exy0 = Math.exp(x1 - y0);
            final double exy1 = Math.exp(x1 - y1);
            final double ey0 = Math.exp(0f - y0);
            final double ey1 = Math.exp(0f - y1);

            return (ey1 - ey0) - (exy1 - exy0);
        }
    }

    // RM: The job of this method is to return cost of placing segment to a particular server
    public double computeSegmentPlacementCost(List<String> tableNameList, List<List<Double>> segmentTimeList,
                                              Double segmentAStartTime,Double segmentAEndTime,String segmentATableName)
    {
        double computedCost = 0.0;
        //Compute the druid metric and return it
        for(int i=0; i<tableNameList.size();i++){
            List<Double> timeList = segmentTimeList.get(i);
            for(int j=0;j<timeList.size();j+=2){
                Double segmentBStart = timeList.get(j);
                Double segmentBEnd = timeList.get(j+1);
                computedCost += computeJointSegmentsCost(segmentAStartTime,segmentAEndTime,segmentATableName,
                        segmentBStart,segmentBEnd,tableNameList.get(i));
            }

        }

        return computedCost;
    }

    @Override
    public double computeInstanceMetric(PinotHelixResourceManager helixResourceManager, IdealState idealState, String instance, String tableName, SegmentMetadata segmentMetadata) {

        ServerPerfMetricsReader serverPerfMetricsReader = new ServerPerfMetricsReader(_executor, _connectionManager, helixResourceManager);
        ServerSegmentInfo serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics(instance, true, 300);

        //fetch list
        List<String> tableNameList = serverSegmentInfo.getTableList();
        List<List<Double>> segmentTimeList = serverSegmentInfo.getSegmentTimeInfo();
        // send values of segment start and end by converting them to seconds.
        Double segmentAStart = (Double.valueOf(segmentMetadata.getTimeInterval().getStartMillis())/1000);
        Double segmentAEnd = (Double.valueOf(segmentMetadata.getTimeInterval().getEndMillis())/1000);
        return computeSegmentPlacementCost(tableNameList,segmentTimeList,segmentAStart,segmentAEnd,segmentMetadata.getTableName());
    }

    @Override
    public void updateServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance, Double currentLoadMetric, String tableName, SegmentMetadata segmentMetadata) {

    }

    @Override
    public void resetServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance) {

    }

    public static void main(String args[]){
        List<String> tables = new ArrayList<String>();

        // created table as name of months
        tables.add("Jan-2018");
        tables.add("Feb-2018");
        tables.add("Mar-2018");

        // epoch time for first day of the month
        Double janStart = 1514764800.0;
        Double febStart = 1517443200.0;
        Double marStart = 1519862400.0;

        // Lists to be sent for computing cost.
        List<List<Double>> segmentTimeList = new ArrayList<>();
        List<Double> timeList = new ArrayList<>();

        // create start and end time for 15 days segments for january
        for(int j=0;j<15;j++) {
            Double temp = janStart + 24*3600;
            timeList.add(janStart);
            timeList.add(temp-1);
            janStart = temp;
        }
        segmentTimeList.add(timeList);

        // create start and end time for 15 days segments for february
        timeList = new ArrayList<Double>();
        for(int j=0;j<15;j++) {
            Double temp = febStart + 24*3600;
            timeList.add(febStart);
            timeList.add(temp-1);
            febStart = temp;
        }
        segmentTimeList.add(timeList);

        // create start and end time for 15 days segments for march
        timeList = new ArrayList<Double>();
        for(int j=0;j<15;j++) {
            Double temp = marStart + 24*3600;
            timeList.add(marStart);
            timeList.add(temp-1);
            marStart = temp;
        }
        segmentTimeList.add(timeList);
        DruidLoadMetric metric = new DruidLoadMetric();
        System.out.println("\nServer contains data from January 1 to 15, February 1 to 15, March 1 to 15");
        // Test adding a new segment for April 2018 (1 month data) into a server that has segments form jan, feb, march
        // This should give a LOW cost value as segment doesn't belong to same tables or overlaps in time.
        Double recv = metric.computeSegmentPlacementCost(tables,segmentTimeList,1522540800.0,
                1525046401.0,"Apr-2018");
        System.out.println("Cost of placing Segment of April: "+recv);

        // Test adding a new segment for March 2018 (1 month data) into a server that has segments form jan, feb, march
        // This should give a HIGH cost value as segment belongs to same table (March) and overlaps in time.
        recv = metric.computeSegmentPlacementCost(tables,segmentTimeList,1519862401.0,
                1522368001.0,"Mar-2018");
        System.out.println("Cost of placing Segment of March: "+recv);
    }
}
