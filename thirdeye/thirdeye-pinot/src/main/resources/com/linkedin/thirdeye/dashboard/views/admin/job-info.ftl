<script id="job-info-template" type="text/x-handlebars-template">
<div class="panel-body">
	<form role="form">
		<label for="job-dataset-selector">Dataset </label> <Select id="job-dataset-selector" name="dataset">
			<option value="---" selected>---</option> 
			<option value="MOST-RECENT">MOST-RECENT</option> 
            {{#each datasets}}
			  <option value="{{this}}">{{this}}</option> 
            {{/each}}
		</Select>
	</form>
</div>
<div class="JobInfoContainer" id="JobInfoContainer-MOST-RECENT"></div>
{{#each datasets}}
<div class="JobInfoContainer" id="JobInfoContainer-{{this}}"></div>
{{/each}}
</script>






