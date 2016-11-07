<script id="metric-config-template" type="text/x-handlebars-template">
<div class="panel-body">
	<form role="form">
		<label for="metric-dataset-selector">Dataset </label> <Select id="metric-dataset-selector" name="dataset">
			<option value="Select Dataset">Select Dataset</option> {{#each datasets}}
			<option value="{{this}}">{{this}}</option> {{/each}}
		</Select>
	</form>
</div>
{{#each datasets}}
<div class="MetricConfigContainer" id="MetricConfigContainer-{{this}}"></div>
{{/each}}
</script>





