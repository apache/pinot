<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <#include "common/style.ftl">
        <#include "common/script.ftl">
        <script src="/assets/js/thirdeye.dashboard.js"></script>
        <script src="/assets/js/thirdeye.sidenav.js"></script>
    </head>
    <body>
        <div class="uk-grid">
            <#-- Side nav -->
            <div id="dashboard-sidenav" class="uk-width-1-4">
                <#include "common/sidenav.ftl">
            </div>

            <div id="dashboard-output" class="uk-width-3-4">
                <div id="dashboard-output-nav" data-uk-sticky>
                    <#include "common/headnav.ftl">
                </div>

                <div id="time-nav-buttons" class="uk-button-group">
                    <button class="uk-button" id="time-nav-left">
                        <i class="uk-icon-angle-left"></i>
                    </button>
                    <button class="uk-button" id="time-nav-right">
                        <i class="uk-icon-angle-right"></i>
                    </button>
                </div>

                
                <div id="dashboard-metric-view">
                    <div id="dashboard-funnels-view">
                        <#include "funnel-heatmap.ftl">
                    </div>

                    <#-- Metric view-->
                    <#if (metricView.type == "INTRA_DAY")>
                        <#include "metric/intra-day.ftl">
                    <#elseif (metricView.type == "TIME_SERIES_FULL" || metricView.type == "TIME_SERIES_OVERLAY")>
                        <#include "metric/time-series.ftl">
                    <#elseif (metricView.type == "FUNNEL")>
                        <#include "metric/funnel.ftl">
                    <#else>
                        <div class="uk-alert uk-alert-danger">
                            <p>
                                No metric view named ${metricView.type}
                            </p>
                        </div>
                    </#if>
                </div>

                <#-- Dimension view-->
                <div id="dashboard-dimension-view">
                    <#if (dimensionView.type == "HEAT_MAP")>
                        <#include "dimension/heat-map.ftl">
                    <#elseif (dimensionView.type == "MULTI_TIME_SERIES")>
                        <#include "dimension/multi-time-series.ftl">
                    <#elseif (dimensionView.type == "TABULAR")>
                        <#include "dimension/tabular.ftl">
                    <#else>
                        <div class="uk-alert uk-alert-danger">
                            <p>
                                No dimension view named ${dimensionView.type}
                            </p>
                        </div>
                    </#if>
                </div>
            </div>
        </div>
    </body>
</html>
