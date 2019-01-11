<html>

<head>
<!-- javascripts -->
<script src="../../../assets/js/vendor/jquery.js" type="text/javascript"></script>
<script src="../../../assets/bootstrap/js/bootstrap.min.js" type="text/javascript"></script>
<script src="../../../assets/jquery-ui/jquery-ui.min.js" type="text/javascript"></script>
<script src="../../../assets/lib/handlebars.min.js" type="text/javascript"></script>
<script src="../../../assets/js/vendor/vendorplugins.compiled.js" type="text/javascript"></script>
<script src="../../../assets/jtable/jquery.jtable.min.js" type="text/javascript"></script>

<!-- select2 -->
<link href="assets/select2/select2.min.css" rel="stylesheet" type="text/css" />
<link href="assets/select2/select2-bootstrap.min.css" rel="stylesheet" type="text/css" />
<script type="text/javascript" src="assets/select2/select2.min.js" defer></script>

<!-- CSS -->
<link href="../../../assets/bootstrap/css/bootstrap.min.css" rel="stylesheet" type="text/css" />
<link href="../../../assets/bootstrap/css/bootstrap-theme.min.css" rel="stylesheet" type="text/css" />
<link href="../../../assets/jquery-ui/jquery-ui.min.css" rel="stylesheet" type="text/css" />
<link href="../../../assets/jtable/themes/metro/blue/jtable.min.css" rel="stylesheet" type="text/css" />
<link href="../../../assets/css/thirdeye.css" rel="stylesheet" type="text/css" />
<link rel="stylesheet" href="../../../assets/css/datatables.min.css"/>

<!-- custom scripts -->
<script src="../../../assets/js/thirdeye/metric-config.js"></script>
<script src="../../../assets/js/thirdeye/dataset-config.js"></script>
<script src="../../../assets/js/thirdeye/job-info.js"></script>

<script src="../../../assets/js/lib/common/utility.js" defer></script>

<!-- JSON Editor comes here-->
<link rel="stylesheet" href="../../../assets/jsonedit/jsoneditor.min.css"/>
<script src="../../../assets/jsonedit/jsoneditor.min.js" defer></script>
<script src="../../../assets/js/lib/entity-editor.js" defer></script>

<script src="../../../assets/js/lib/self-service-mappings.js" defer></script>

<script type="text/javascript">

  $(document).ready(function() {
    //compile templates


    // checking if user is authenticated
    // redirects to the login screen if not
    $.get('/auth/')
        .done(function() {
          var metric_config_template = $("#metric-config-template").html();
          metric_config_template_compiled = Handlebars.compile(metric_config_template);

          var job_info_template = $("#job-info-template").html();
          job_info_template_compiled = Handlebars.compile(job_info_template);

          //register callbacks on tabs
          $('a[data-toggle="tab"]').on('shown.bs.tab', function(e) {
            e.target // newly activated tab
            e.relatedTarget // previous active tab
            var tabId = $(e.target).attr("href")
            resolveHash(tabId);
          })
        })
      .fail(function() {
          window.location.replace('/app/#/login');
      })
  });

	function resolveHash(tabId) {
    $(tabId).tab('show');
    if (tabId == "#metric-config") {
      showMetricDatasetSelection();
    }
    if (tabId == "#dataset-config") {
      listDatasetConfigs();
    }
    if(tabId == "#job-info"){
      listJobs();
    }
    if(tabId == "#entity-editor"){
      renderConfigSelector();
    }
    if(tabId=="#mappings") {
      renderMappingsSelection();
    }
	}

</script>
<#include "dataset-config.ftl"/>
<#include "metric-config.ftl"/>
<#include "job-info.ftl"/>
</head>
<body>
	<div class="container-fluid">
		<nav class="navbar navbar-inverse">
			<div class="navbar-header">
				<button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
					<span class="sr-only">Toggle navigation</span> <span class="icon-bar"></span> <span class="icon-bar"></span> <span class="icon-bar"></span>
				</button>
				<a class="navbar-brand" href="#">ThirdEye</a>
			</div>
			<div id="navbar" class="collapse navbar-collapse">
				<ul class="nav navbar-nav">
					<li class=""><a href="#dataset-config" data-toggle="tab">Dataset </a></li>
					<li class=""><a href="#metric-config" data-toggle="tab">Metric</a></li>
					<li class=""><a href="#job-info" data-toggle="tab">JobInfo</a></li>
          <li class=""><a href="#entity-editor" data-toggle="tab">Entity Editor</a></li>
          <li class=""><a href="#mappings" data-toggle="tab">Mappings</a></li>
				</ul>

				<ul class="nav navbar-nav navbar-right">
					<li><a href="#">Manage Anomalies</a></li>
					<li><a href="#">Sign In</a></li>
				</ul>
			</div>
		</nav>
	</div>
	<div class="container-fluid">
		<div class="tab-content">
			<div class="tab-pane" id="dataset-config">
				<div id="dataset-config-place-holder"></div>
			</div>
			<div class="tab-pane" id="metric-config">
				<div id="metric-config-place-holder"></div>
			</div>
			<div class="tab-pane" id="job-info">
				<div id="job-info-place-holder"></div>
			</div>
      <div class="tab-pane" id="entity-editor">
        <div id="entity-editor-place-holder"></div>
      </div>
      <div class="tab-pane" id="mappings">
        <div id="mappings-place-holder"></div>
      </div>
		</div>
	</div>
</body>
</html>
