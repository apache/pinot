/**
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
package org.apache.pinot.queries;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.config.QueryExecutorConfig;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.env.PinotConfiguration;

import static org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2.USE_DICTIONARY_FOR_DISTINCT;

/**
 * Base class for tests that wish to use DISTINCT with Dictionary based plan
 */
public abstract class BaseQueriesWithDictBasedDistinctEnabledTest extends BaseQueriesTest {

    protected static PlanMaker PLAN_MAKER_WITH_DICTAGG;

    public BaseQueriesWithDictBasedDistinctEnabledTest() {
        PropertiesConfiguration queryExecutorConfig = new PropertiesConfiguration();

        queryExecutorConfig.setDelimiterParsingDisabled(false);
        queryExecutorConfig.setProperty(USE_DICTIONARY_FOR_DISTINCT, true);

        try {
            PLAN_MAKER_WITH_DICTAGG = new InstancePlanMakerImplV2(new QueryExecutorConfig(new PinotConfiguration(queryExecutorConfig)));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Run PQL query on single index segment with the Dictionary based Distinct Plan enabled.
     * <p>Use this to test a single operator.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected <T extends Operator> T getOperatorForPqlQueryWithDictBasedDistinct(String pqlQuery) {
        QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromPQL(pqlQuery);
        return (T) PLAN_MAKER_WITH_DICTAGG.makeSegmentPlanNode(getIndexSegment(), queryContext).run();
    }

    /**
     * Run SQL query on single index segment with dictionary based distinct plan enabled.
     * <p>Use this to test a single operator.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected <T extends Operator> T getOperatorForSqlQueryWithDictBasedDistinct(String sqlQuery) {
        QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(sqlQuery);
        return (T) PLAN_MAKER_WITH_DICTAGG.makeSegmentPlanNode(getIndexSegment(), queryContext).run();
    }
}
