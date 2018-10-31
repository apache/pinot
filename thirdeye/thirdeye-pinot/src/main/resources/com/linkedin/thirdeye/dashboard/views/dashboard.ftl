<!DOCTYPE html>
<html>

<#include "head.ftl">

<body>
<div id="main-view">
  <nav class="tm-navbar uk-navbar uk-navbar-attached">
    <div class="uk-container uk-container-center">
      <a class="uk-navbar-brand brand" href="/dashboard">ThirdEye</a>

      <ul class="header-tabs uk-navbar-nav uk-hidden-small" data-uk-switcher="{connect:'#tabs'}">
        <li id="dashboard-header-tab" class="header-tab" rel="dashboard"><a href="#">Dashboard</a>
        </li>
        <li id="compare-header-tab" class="header-tab" rel="compare"><a href="#">Time over time
          comparison</a></li>
        <li id="timeseries-header-tab" class="header-tab" rel="timeseries"><a
            href="#">Timeseries</a></li>
        <li id="anomalies-header-tab" class="header-tab" rel="anomalies"><a href="#">Anomalies</a>
        </li>
        <li id="self-service-header-tab" class="header-tab" rel="self-service"><a href="#">Self
          Service</a></li>
        <li id="new-ui-tab" class="header-tab header-tab--secondary"><button class="newui-button">New UI</button></li>
      </ul>

      <a id="header-help" class="data-uk-tooltip uk-float-right" title="How to use ThirdEye?"
         href="https://docs.google.com/document/d/1w4I2fR78pV1fZUqhTm0yILGrQJvXmNZZhBkdU25IWRI/"
         target="_blank">
        <i class=" uk-icon-question-circle uk-icon-medium"></i>
      </a>
    </div>
  </nav>

  <ul id="tabs" class="uk-switcher">
    <li id="dashboard"></li>
    <li id="compare"></li>
    <li id="timeseries"></li>
    <li id="anomalies"></li>
    <li id="self-service"></li>
  </ul>
</div>

<#include "tabs/tab.ftl">
<#include "tabs/common/form.ftl">
<#include "tabs/common/dataset-list.ftl">
<#include "tabs/common/metric-list.ftl">
<#include "tabs/common/filter-value.ftl">
<#include "tabs/compare-tab/metric-timeseries.ftl">
<#include "tabs/compare-tab/tabular.ftl">
<#include "tabs/compare-tab/heat-map.ftl">
<#include "tabs/compare-tab/heat-map-summary.ftl">
<#include "tabs/compare-tab/contributors.ftl">
<#include "tabs/timeseries-tab/timeseries.ftl">
<#include "tabs/anomalies-tab/anomalies.ftl">
<#include "tabs/anomalies-tab/anomaly-details.ftl">
<#include "tabs/self-service-tab/self-service.ftl">
<#include "tabs/self-service-tab/anomaly-function-form.ftl">
<#include "tabs/self-service-tab/self-service-existing-functions.ftl">
<#include "tabs/self-service-tab/self-service-email.ftl">

</body>

</html>
