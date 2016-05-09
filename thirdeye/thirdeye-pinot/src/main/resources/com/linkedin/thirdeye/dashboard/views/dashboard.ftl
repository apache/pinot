<!DOCTYPE html>
<html>

<#include "head.ftl">

<body>
<div id="main-view">
	<nav class="tm-navbar uk-navbar uk-navbar-attached">
		<div class="uk-container uk-container-center">
            <a class="uk-navbar-brand brand" href="/dashboard#">Thirdeye</a>

			<ul class="header-tabs uk-navbar-nav uk-hidden-small" data-uk-switcher="{connect:'#tabs'}">
				<li id="dashboard-header-tab" class="header-tab" rel="dashboard"><a href="#">Dashboard</a></li>
				<li id="compare-header-tab" class="header-tab" rel="compare"><a href="#">Time over time comparison</a></li>
				<li id="timeseries-header-tab" class="header-tab" rel="timeseries"><a href="#">Timeseries</a></li>
				<li id="anomalies-header-tab" class="header-tab" rel="anomalies"><a href="#">Anomalies</a></li>
			</ul>
		</div>
	</nav>

	<ul id="tabs" class="uk-switcher">
		<li id="dashboard"></li>
		<li id="compare"></li>
		<li id="timeseries"></li>
		<li id="anomalies"></li>
	</ul>
</div>

    <#include "tabs/tab.ftl">
    <#include "form.ftl">
    <#include "dataset-list.ftl">
    <#include "filter-value.ftl">
    <#include "metric-timeseries.ftl">
    <#include "tabular.ftl">
    <#include "heat-map.ftl">
    <#include "contributors.ftl">
    <#include "timeseries.ftl">
    <#include "anomalies.ftl">


</body>

</html>