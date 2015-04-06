<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">

        <link rel="stylesheet" href="/assets/stylesheets/uikit/uikit.almost-flat.min.css"/>
        <link rel="stylesheet" href="/assets/stylesheets/uikit/components/form-select.min.css"/>
        <link rel="stylesheet" href="/assets/stylesheets/uikit/components/datepicker.almost-flat.min.css"/>
        <link rel="stylesheet" href="/assets/stylesheets/uikit/components/autocomplete.almost-flat.min.css"/>
        <link rel="stylesheet" href="/assets/stylesheets/dashboard.css"/>
        <link rel="stylesheet" href="/assets/stylesheets/jquery-ui.css"/>

        <script src="/assets/javascripts/vendor/jquery.js"></script>
        <script src="/assets/javascripts/vendor/jquery-ui.js"></script>
        <script src='/assets/javascripts/vendor/globalize.js'></script>
        <script src='/assets/javascripts/vendor/moment.js'></script>
        <script src='/assets/javascripts/jquery.flot.js'></script>
        <script src='/assets/javascripts/jquery.flot.time.js'></script>
        <script src="/assets/javascripts/uikit/uikit.min.js"></script>
        <script src="/assets/javascripts/uikit/components/form-select.min.js"></script>
        <script src="/assets/javascripts/uikit/components/datepicker.min.js"></script>
        <script src="/assets/javascripts/uikit/components/autocomplete.min.js"></script>
        <script src="/assets/javascripts/uikit/components/timepicker.min.js"></script>
    </head>
    <body>
        <div id="nav-bar-area">
            <#include "nav-bar-component.ftl"/>
        </div>
        <div id="input-component-area">
            <#include "input-component.ftl"/>
        </div>

        <div id="display-area">
            <div id="breadcrumbs-area">
                <#include "breadcrumbs-component.ftl"/>
            </div>
            <div id="time-series-component-area">
                <#include "time-series-component.ftl"/>
            </div>
            <#if funnels??>
                <div id="funnel-nav-component-area">
                    <#include "funnel-nav-component.ftl"/>
                </div>
                <div id="funnel-component-area">
                    <#include "funnel-component.ftl"/>
                </div>
            </#if>
            <div id="heat-map-nav-area">
                <#include "heat-map-nav-component.ftl"/>
            </div>
            <div id="heat-map-component-area">
                <#include "heat-map-component.ftl"/>
            </div>
        </div>

        <script>
            $(document).ready(function() {
                plotTimeSeries("time-series-component-area")
            })
        </script>
    </body>
</html>
