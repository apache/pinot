<div class="container-fluid bg-white row-bordered">
	<div class="container top-buffer bottom-buffer ">

		<label for="create-dashboard-button" class="view-anomaly-label">Dashboard:</label>
		<button type="button" class="btn btn-link" id="create-dashboard-button">+Create New Dashboard</button>
	</div>
</div>

<div class="container-fluid bg-white row-bordered">
	<div class="container top-buffer bottom-buffer ">

		<div>
			<label style="font-size: 15px; font-weight: 500">Select time range: </label>
		</div>
		<div>
			<label style="font-size: 11px; font-weight: 500">DATE RANGE(CURRENT) </label>
		</div>

		<div id="reportrange">
			<i class="glyphicon glyphicon-calendar fa fa-calendar"></i>&nbsp; <span></span> <b class="caret"></b>
		</div>
	</div>
</div>
<div class="container-fluid">

	<div class="container top-buffer bottom-buffer">
		<div>100 Metrics</div>
	</div>
	<div class="container">
		<nav class="navbar navbar-transparent" role="navigation">
			<div class="navbar-header">
				<a class="navbar-brand">View metrics by:</a>
			</div>
			<div id="navbar" class="collapse navbar-collapse">
				<ul class="nav navbar-nav">
					<li class=""><a class="hvr-underline-from-center" href="#num_of_anomalies" data-toggle="tab"># of Anomalies</a></li>
					<li class=""><a class="hvr-underline-from-center" href="#wow" data-toggle="tab">WoW</a></li>
				</ul>
			</div>
		</nav>
	</div>
	<div class="tab-content">
		<div class="tab-pane" id="num_of_anomalies">
			<div id="num_of_anomalies-place-holder">
				<#include "dashboard/num_of_anomalies_dashboard.ftl"/>
			</div>
		</div>
		<div class="tab-pane" id="wow">
			<div id="wow-place-holder">
				<#include "dashboard/WoW_dashboard.ftl"/>
			</div>
		</div>
	</div>
</div>
