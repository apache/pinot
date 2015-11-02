<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <#include "common/style.ftl">
        <#include "common/script.ftl">

        <script src="/assets/js/thirdeye.dashboard.js"></script>
        <script src="/assets/js/thirdeye.sidenav.js"></script>
        <script src="/assets/js/thirdeye.configform.js"></script>
    </head>
    <body>
        <div class="uk-grid">
            <!-- Side nav will be not included in the new design hidden for now-->
            <#-- Side nav -->
            <div id="dashboard-sidenav" class="uk-hidden">
                <#include "common/sidenav.ftl">
            </div>

            <div id="dashboard-output" class="uk-width-1-1">
                <div id="dashboard-output-nav" data-uk-sticky>
                    <#include "common/headnav.ftl">
                </div>

               <#-- Dimension view-->
                <div id="dashboard-dimension-view">
                    <#include "common/config-form.ftl">

                    <#if (dimensionView.type == "HEAT_MAP")>
                        <#include "dimension/heat-map.ftl">
                    <#elseif (dimensionView.type == "MULTI_TIME_SERIES")>
                        <#include "metric/time-series.ftl">
                        <#include "dimension/multi-time-series.ftl">
                    <#elseif (dimensionView.type == "TABULAR")>
                        <div id="dashboard-funnels-view">
                            <#include "funnel-heatmap.ftl">
                        </div>
                            <#--<#include "metric/intra-day.ftl">-->

                            <#--<#include "dimension/tabular.ftl">-->
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
