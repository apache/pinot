<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-12">
					<div style="float: left;">
						<label for="dashboard-name-input" class="label-large-light">Dashboard Name: </label>
					</div>
					<div style="width: 370px; float: left">
						<select style="width: 100%" id="dashboard-name-input" class="label-large-light underlined">
						{{#if this.dashboardName}}
						   <option value="{{this.dashboardId}}" selected>{{this.dashboardName}}</option>
						{{/if}}
						</select>
					</div>
					<div style="float: left">
						<a type="button" class="btn btn-link label-medium-semibold" id="create-dashboard-button" data-toggle="modal" data-target="#create-dashboard-modal"><span class="glyphicon glyphicon-cog"
							aria-hidden="true"></span></a>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
<div id="dashboard-content" class="container-fluid" style="display:none">
	<div class="row row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div>{{this.dashboardName}}</div>
		</div>
		<div class="container">
			<nav class="navbar navbar-transparent" role="navigation">
				<div id="dashboard-tabs" class="collapse navbar-collapse">
					<ul class="nav navbar-nav dashboard-tabs" id="dashboard-tabs">
					    <li class=""><a href="#dashboard_metric-summary-tab">Metric Health by last 24 hours</a></li>
						<li class=""><a href="#dashboard_anomaly-summary-tab"># of Anomalies</a></li>
						<li class=""><a href="#dashboard_wow-summary-tab">Week Over Week</a></li>
					</ul>
				</div>
			</nav>
		</div>
		<div class="tab-content">
			<div class="tab-pane" id="dashboard_metric-summary-tab">
				<div id="metric-summary-place-holder"></div>
  			</div>
			<div class="tab-pane" id="dashboard_anomaly-summary-tab">
				<div id="anomaly-summary-place-holder"></div>
			</div>
			<div class="tab-pane" id="dashboard_wow-summary-tab">
				<div id="wow-place-holder"></div>
			</div>
		</div>
	</div>
</div>
<div id='summary-spin-area'></div>

<#include "dashboard/manage-dashboard-modal.ftl"/>

