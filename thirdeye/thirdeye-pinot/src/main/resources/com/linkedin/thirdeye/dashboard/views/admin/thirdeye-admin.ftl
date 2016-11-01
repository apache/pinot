<html>

<head>
<!-- javascripts -->
<script src="../../../assets/js/vendor/jquery.js" type="text/javascript"></script>
<script src="../../../assets/bootstrap/js/bootstrap.min.js" type="text/javascript"></script>
<script src="../../../assets/jquery-ui/jquery-ui.min.js" type="text/javascript"></script>
<script src="../../../assets/lib/handlebars.min.js" type="text/javascript"></script>
<script src="../../../assets/js/vendor/vendorplugins.compiled.js" type="text/javascript"></script>
<script src="../../../assets/jtable/jquery.jtable.min.js" type="text/javascript"></script>

<!-- CSS -->
<link href="../../../assets/bootstrap/css/bootstrap.min.css" rel="stylesheet" type="text/css" />
<link href="../../../assets/bootstrap/css/bootstrap-theme.min.css" rel="stylesheet" type="text/css" />
<link href="../../../assets/jquery-ui/jquery-ui.min.css" rel="stylesheet" type="text/css" />
<link href="../../../assets/jtable/themes/metro/blue/jtable.min.css" rel="stylesheet" type="text/css" />

<!-- custom scripts -->
<script src="../../../assets/js/lib/ingraph-metric-config.js"></script>
<script src="../../../assets/js/lib/metric-config.js"></script>
<script src="../../../assets/js/lib/dataset-config.js"></script>
<script src="../../../assets/js/lib/job-info.js"></script>

<script type="text/javascript">
  $(document).ready(function() {
    //compile templates
    var ingraph_metric_config_template = $("#ingraph-metric-config-template").html();
    ingraph_metric_config_template_compiled = Handlebars.compile(ingraph_metric_config_template);
    
    var metric_config_template = $("#metric-config-template").html();
    metric_config_template_compiled = Handlebars.compile(metric_config_template);
    
    var job_info_template = $("#job-info-template").html();
    job_info_template_compiled = Handlebars.compile(job_info_template);
    
    //register callbacks on tabs
    $('a[data-toggle="tab"]').on('shown.bs.tab', function (e) {
      e.target // newly activated tab
      e.relatedTarget // previous active tab
      tabId = $(e.target).attr("href")
      $(tabId).tab('show')
      if(tabId == "#ingraph-config"){
      	showIngraphDatasetSelection();
      }
      if(tabId == "#metric-config"){
      	showMetricDatasetSelection();
      }
      if(tabId == "#dataset-config"){
      	listDatasetConfigs();
      }
      if(tabId == "#job-info"){
      	listJobs();
      }
    })
  });
</script>
</head>
<body>
	<div class="container-fluid">
		<ul class="nav nav-tabs navbar-inverse">
			<li class=""><a href="#ingraph-config" data-toggle="tab">Ingraph Metric</a></li>
			<li class=""><a href="#dataset-config" data-toggle="tab">Dataset </a></li>
			<li class=""><a href="#metric-config" data-toggle="tab">Metric</a></li>
			<li class=""><a href="#job-info" data-toggle="tab">JobInfo</a></li>
		</ul>
		<div class="tab-content">
			<div class="tab-pane" id="ingraph-config">
			   <#include "ingraph-metric-config.ftl">
			   <div id="ingraph-metric-config-place-holder"></div>
			</div>
			<div class="tab-pane" id="dataset-config">
			 <#include "dataset-config.ftl">
			   <div id="dataset-config-place-holder"></div>
			</div>
			<div class="tab-pane" id="metric-config">
   			   <#include "metric-config.ftl">
			   <div id="metric-config-place-holder"></div>
			</div>
			<div class="tab-pane" id="job-info">
				<#include "job-info.ftl">
			   <div id="job-info-place-holder"></div>
			</div>
		</div>
	</div>
</body>
</html>