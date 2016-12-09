<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-12">
					<div style="float: left;">
						<label for="dashboard-input" class="label-large-light">Dashboard Name: </label>
					</div>
          <div style="width:370px;float: left">
            <input type="text" name="dashboard" size="35" id="dashboard-input" class="label-large-light underlined" placeholder="Search for a Dashboard"/>
          </div>

				  <div style="float: left">
						<a type="button" class="btn btn-link label-medium-semibold" id="create-dashboard-button" data-toggle="modal" data-target="#create-dashboard-modal">+ Create Custom Dashboard</a>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div>
				<label class="label-medium-semibold">Select time range: </label>
			</div>
			<div>
				<label class="label-small-semibold">DATE RANGE(CURRENT) </label>
			</div>

			<div id="dashboard-time-range">
				<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>&nbsp; <span></span> <b class="caret"></b>
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
				<div class="navbar-header">
					<a class="navbar-brand"><label class="label-medium-semibold">View metrics by:</label></a>
				</div>
				<div id="dashboard-tabs" class="collapse navbar-collapse">
					<ul class="nav navbar-nav dashboard-tabs" id="dashboard-tabs">
						<li class=""><a href="#anomaly-summary-tab" data-toggle="tab"># of Anomalies</a></li>
						<li class=""><a href="#wow-summary-tab" data-toggle="tab">Week Over Week</a></li>
					</ul>
				</div>
			</nav>
		</div>
		<div class="tab-content">
			<div class="tab-pane in active" id="anomaly-summary-tab">
				<div id="anomaly-summary-place-holder">
				</div>
			</div>
			<div class="tab-pane fade" id="wow-summary-tab">
				<div id="wow-place-holder">
				</div>
			</div>
		</div>
	</div>
</div>
{{/if}}

<#include "dashboard/manage-dashboard-modal.ftl"/>

