<html>

<head>



<!-- CSS -->

<link href="../../assets/bootstrap/css/bootstrap.min.css" rel="stylesheet" type="text/css" />
<link href="../../assets/bootstrap/css/bootstrap-theme.min.css" rel="stylesheet" type="text/css" />
<link href="../../assets/jquery-ui/jquery-ui.min.css" rel="stylesheet" type="text/css" />
<link href="../../assets/jtable/themes/metro/blue/jtable.min.css" rel="stylesheet" type="text/css" />
<link href="../../assets/chosen/chosen.min.css" rel="stylesheet" type="text/css" />
<link rel="stylesheet" href="../../../assets/css/d3.css" />
<link rel="stylesheet" href="../../../assets/css/c3.css" />
<link rel="stylesheet" type="text/css" href="../../../assets/typeahead/typeaheadjs.css" />
<link rel="stylesheet" type="text/css" href="../../../assets/tokenfield/css/bootstrap-tokenfield.css" />

<link rel="stylesheet" type="text/css" href="//cdn.jsdelivr.net/bootstrap.daterangepicker/2/daterangepicker.css" />

<link href="../../assets/css/styles.css" rel="stylesheet" type="text/css" />
<link href="../../assets/css/thirdeye.css" rel="stylesheet" type="text/css" />

<!-- javascripts -->
<script src="../../assets/js/vendor/jquery.js" type="text/javascript"></script>
<script src="../../assets/bootstrap/js/bootstrap.min.js" type="text/javascript"></script>
<script src="../../assets/jquery-ui/jquery-ui.min.js" type="text/javascript"></script>
<script src="../../assets/lib/handlebars.min.js" type="text/javascript"></script>
<script src="../../assets/js/vendor/vendorplugins.compiled.js" type="text/javascript"></script>
<script src="../../assets/chosen/chosen.jquery.min.js" type="text/javascript"></script>
<script src="../../assets/jtable/jquery.jtable.min.js" type="text/javascript"></script>
<script src="../../assets/js/d3/d3.v3.min.js" charset="utf-8" defer></script>
<script src="../../assets/js/c3/c3.js" defer></script>
<script type="text/javascript" src="//cdn.jsdelivr.net/bootstrap.daterangepicker/2/daterangepicker.js"></script>
<script type="text/javascript" src="../../assets/typeahead/typeahead.js"></script>
<script type="text/javascript" src="../../assets/tokenfield/bootstrap-tokenfield.min.js"></script>

<!-- custom scripts -->
<script src="../../assets/js/thirdeye/ingraph-metric-config.js"></script>
<script src="../../assets/js/thirdeye/ingraph-dashboard-config.js"></script>
<script src="../../assets/js/thirdeye/metric-config.js"></script>
<script src="../../assets/js/thirdeye/dataset-config.js"></script>
<script src="../../assets/js/thirdeye/job-info.js"></script>
<script src="../../assets/js/thirdeye/anomalies.js"></script>
<script id="anomalies-template" type="text/x-handlebars-template">
  <#include "tabs/anomalies.ftl"/>
</script>
<script id="dashboard-template" type="text/x-handlebars-template">
  <#include "tabs/dashboard.ftl"/>
</script>
<script id="analysis-template" type="text/x-handlebars-template">
  <#include "tabs/analysis.ftl"/>
</script>
<#include "admin/job-info.ftl"/>
<#include "admin/ingraph-metric-config.ftl"/>
<#include "admin/ingraph-dashboard-config.ftl"/>
<#include "admin/dataset-config.ftl"/>
<#include "admin/metric-config.ftl"/>
<#include "admin/job-info.ftl"/>
<script type="text/javascript">
  $(document).ready(function() {
    //compile templates
    var dashboard_template = $("#dashboard-template").html();
    dashboard_template_compiled = Handlebars.compile(dashboard_template);

    var anomalies_template = $("#anomalies-template").html();
    anomalies_template_compiled = Handlebars.compile(anomalies_template);

    var analysis_template = $("#analysis-template").html();
    analysis_template_compiled = Handlebars.compile(analysis_template);

    var ingraph_metric_config_template = $("#ingraph-metric-config-template").html();
    ingraph_metric_config_template_compiled = Handlebars.compile(ingraph_metric_config_template);

    var metric_config_template = $("#metric-config-template").html();
    metric_config_template_compiled = Handlebars.compile(metric_config_template);

    var job_info_template = $("#job-info-template").html();
    job_info_template_compiled = Handlebars.compile(job_info_template);

    if (location.hash !== '') {
      $('a[href="' + location.hash + '"]').tab('show');
    }

    //register callbacks on tabs
    $('a[data-toggle="tab"]').on('shown.bs.tab', function(e) {
      e.target // newly activated tab
      e.relatedTarget // previous active tab
      tabId = $(e.target).attr("href")
      $(tabId).tab('show')
      if (tabId == "#dashboard") {
        var result_dashboard_template_compiled = dashboard_template_compiled({});
        $("#dashboard-place-holder").html(result_dashboard_template_compiled);
      }
      if (tabId == "#anomalies") {
        var result_anomalies_template_compiled = anomalies_template_compiled({});
        $("#anomalies-place-holder").html(result_anomalies_template_compiled);
        $('.chosen-select').chosen();
        $('input[id="anomalies-date-range-selector"]').daterangepicker();
        renderAnomalies();
      }
      if (tabId == "#analysis") {
        var result_analysis_template_compiled = analysis_template_compiled({});
        $("#analysis-place-holder").html(result_analysis_template_compiled);
      }

      if (tabId == "#ingraph-metric-config") {
        showIngraphDatasetSelection();
      }
      if (tabId == "#ingraph-dashboard-config") {
        listIngraphDashboardConfigs();
      }
      if (tabId == "#metric-config") {
        showMetricDatasetSelection();
      }
      if (tabId == "#dataset-config") {
        listDatasetConfigs();
      }
      if (tabId == "#job-info") {
        listJobs();
      }
    })
  });
</script>

</head>
<body>

	<div class="container-fullwidth bg-black">
		<div class="row">
			<div class="col-md-12">
				<div class="container">
					<nav class="navbar navbar-inverse" role="navigation">
						<div class="navbar-header">
							<button type="button" class="navbar-toggle" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1">
								<span class="sr-only">Toggle navigation</span><span class="icon-bar"></span><span class="icon-bar"></span><span class="icon-bar"></span>
							</button>
							<a class="navbar-brand" href="#">ThirdEye</a>
						</div>
						<div id="navbar" class="collapse navbar-collapse">
							<ul class="nav navbar-nav">
								<li class=""><a class="hvr-underline-from-center" href="#dashboard" data-toggle="tab">Dashboard</a></li>
								<li class=""><a class="hvr-underline-from-center" href="#anomalies" data-toggle="tab">Anomalies</a></li>
								<li class=""><a class="hvr-underline-from-center" href="#analysis" data-toggle="tab">Root Cause Analysis</a></li>
							</ul>

							<ul class="nav navbar-nav navbar-right">
								<li><a href="#">Manage Anomalies</a></li>
								<li class="dropdown"><a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">Admin <span class="caret"></span></a>
									<ul class="dropdown-menu">
										<li class=""><a href="#ingraph-metric-config" data-toggle="tab">Ingraph Metric</a></li>
										<li class=""><a href="#ingraph-dashboard-config" data-toggle="tab">Ingraph Dashboard</a></li>
										<li class=""><a href="#dataset-config" data-toggle="tab">Dataset </a></li>
										<li class=""><a href="#metric-config" data-toggle="tab">Metric</a></li>
										<li class=""><a href="#job-info" data-toggle="tab">JobInfo</a></li>
									</ul></li>
								<li><a href="#">Sign In</a></li>
							</ul>
						</div>
					</nav>
				</div>
			</div>
		</div>
	</div>

	<div class="tab-content">
		<div class="tab-pane" id="dashboard">
			<div id="dashboard-place-holder"></div>
		</div>
		<div class="tab-pane" id="anomalies">
			<div id="anomalies-place-holder"></div>
		</div>
		<div class="tab-pane" id="analysis">
			<div id="analysis-place-holder"></div>
		</div>

		<div class="tab-pane" id="ingraph-metric-config">
			<div id="ingraph-metric-config-place-holder"></div>
		</div>
		<div class="tab-pane" id="ingraph-dashboard-config">
			<div id="ingraph-dashboard-config-place-holder"></div>
		</div>
		<div class="tab-pane" id="dataset-config">
			<div id="dataset-config-place-holder"></div>
		</div>
		<div class="tab-pane" id="metric-config">
			<div id="metric-config-place-holder"></div>
		</div>
		<div class="tab-pane" id="job-info">
			<div id="job-info-place-holder"></div>
		</div>
	</div>
</body>
</html>