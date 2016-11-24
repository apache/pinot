<div class="container-fluid">
	<div class="row bg-white row-bordered">
		<div class="container top-buffer bottom-buffer ">
			<div class=row>
				<div class="col-md-12">
					<div style="float: left;">
						<label for="create-dashboard-button" class="label-large-light">Dashboard:</label>
					</div>
					<div style="width:370px;float: left">
						<select id="create-dashboard-button" class="form-control underlined label-large-light">
							<option>100 most recent anomalies</option>
						</select>
					</div>
					<div style="float: left">
						<button type="button" class="btn btn-link label-medium-semibold" id="create-dashboard-button">+Create Custom Dashboard</button>
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
				<div class="collapse navbar-collapse">
					<ul class="nav navbar-nav dashboard-tabs" id="dashboard-tabs">
						<li class=""><a href="#num_of_anomalies" data-toggle="tab"># of Anomalies</a></li>
						<li class=""><a href="#wow" data-toggle="tab">Week Over Week</a></li>
					</ul>
				</div>
			</nav>
		</div>
		<div class="tab-content">
			<div class="tab-pane active in" id="num_of_anomalies">
				<div id="num_of_anomalies-place-holder">
					<#include "dashboard/num-of-anomalies-dashboard.ftl"/>
				</div>
			</div>
			<div class="tab-pane" id="wow">
				<div id="wow-place-holder">
					<#include "dashboard/wow-dashboard.ftl"/>
				</div>
			</div>
		</div>
	</div>
</div>
