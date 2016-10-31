<section id="ingraph-metric-config-section" width="100%">
	<script id="ingraph-metric-config-template" type="text/x-handlebars-template">
<div class="panel-body">
	<form role="form">
		<label for="ingraph-dataset-selector">Dataset </label> <Select id="ingraph-dataset-selector" name="dataset">
			<option value="Select Dataset">Select Dataset</option> {{#each datasets}}
			<option value="{{this}}">{{this}}</option> {{/each}}
		</Select>
	</form>
</div>
{{#each datasets}}
<div class="IngraphMetricContainer" id="IngraphMetricContainer-{{this}}"></div>
{{/each}}
</script>
</section>





