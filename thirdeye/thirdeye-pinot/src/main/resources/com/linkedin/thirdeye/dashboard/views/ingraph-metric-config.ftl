<html>
<head>
<script src="../../../assets/js/vendor/jquery.js"></script>
<script src="../../../assets/jquery-ui/jquery-ui.min.js"></script>
<script src="../../../assets/lib/handlebars.min.js"></script>
<script src="../../../assets/js/vendor/vendorplugins.compiled.js"></script>

<!-- Include jTable script file. -->
<script src="../../../assets/jtable/jquery.jtable.min.js"
	type="text/javascript"></script>
<script src="../../../assets/js/lib/ingraph-metric-config.js"></script>

<link href="../../../assets/jquery-ui/jquery-ui.min.css"
	rel="stylesheet" type="text/css" />
<link href="../../../assets/jtable/themes/metro/blue/jtable.min.css"
	rel="stylesheet" type="text/css" />


<script type="text/javascript">
  $(document).ready(
      function() {
        var ingraph_metric_config_template = $(
            "#ingraph-metric-config-template").html();
        ingraph_metric_config_template_compiled = Handlebars
            .compile(ingraph_metric_config_template);
        showDatasetSelection();
      });
</script>
</head>
<body>
	<section id="ingraph-metric-config-section" width="100%">
		<script id="ingraph-metric-config-template"
			type="text/x-handlebars-template">
		<div>
      <label class="uk-form-label">Dataset </label>
      <Select id="ingraph-dataset-selector" name="dataset">
      <option value="Select Dataset">Select Dataset</option> 
			{{#each datasets}}
				<option value="{{this}}">{{this}}</option> 
			{{/each}}
			</Select>
		</div>
    {{#each datasets}}
      <div class="IngraphMetricContainer" id="IngraphMetricContainer-{{this}}"></div>
    {{/each}}

	  </script>
	</section>
	<div id="ingraph-metric-config-place-holder"></div>

</body>
</html>
