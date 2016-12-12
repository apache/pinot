<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-12">
					<div style="float: left;">
						<label for="dashboard-name-input" class="label-large-light">Dashboard Name: </label>
					</div>
					<div style="width: 370px; float: left">
						<select style="width: 100%" id="dashboard-name-input" class="label-large-light underlined"></select>
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
{{#if this.dashboardName}}
<div class="container-fluid">
	<div class="row row-bordered">
		<div class="container top-buffer bottom-buffer">
			<div>100 Metrics</div>
		</div>
		<div class="container">
			<nav class="navbar navbar-transparent" role="navigation">
				<div class="navbar-header" >
					<label class="label-medium-semibold" style="padding-top:15px;padding-bottom:15px">View metrics by:</label>
				</div>
				<div id="dashboard-tabs" class="collapse navbar-collapse">
					<ul class="nav navbar-nav dashboard-tabs" id="dashboard-tabs">
						<li class=""><a href="#dashboard_anomaly-summary-tab"># of Anomalies</a></li>
						<li class=""><a href="#dashboard_wow-summary-tab">Week Over Week</a></li>
					</ul>
				</div>
			</nav>
		</div>
		<div class="tab-content">
			<div class="tab-pane" id="dashboard_anomaly-summary-tab">
				<div id="anomaly-summary-place-holder"></div>
			</div>
			<div class="tab-pane" id="dashboard_wow-summary-tab">
				<div id="wow-place-holder"></div>
			</div>
		</div>
	</div>
</div>
{{/if}}

<#include "dashboard/manage-dashboard-modal.ftl"/>

