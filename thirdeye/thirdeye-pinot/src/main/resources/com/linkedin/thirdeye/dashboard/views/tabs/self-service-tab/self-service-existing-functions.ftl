<section id="self-service-existing-anomaly-functions">
    <script id="self-service-existing-anomaly-functions-template" type="text/x-handlebars-template">
        <table id="existing-anomaly-functions-table" class="display">
            <thead>
            <tr>
                <th>Name</th>
                <th>Metric</th>
                <th>Properties</th>
                <th>Filter</th>
                <th>Active</th>
                <th>Edit</th>
                <th>Delete</th>
            </tr>
            </thead>
            <tbody id="existing-anomaly-functions-tbody">
            {{#each this as |anomalyFunction anomalyFunctionIndex|}}
            <tr class="existing-function-row" data-function-id="{{anomalyFunction/id}}">
                <td>{{anomalyFunction/functionName}}</td>
                <td>{{anomalyFunction/metric}}</td>
                <td>{{{listAnomalyProperties anomalyFunction/properties}}}</td>
                <td>{{anomalyFunction/filters}}</td>
                <td><input type="checkbox" {{#if anomalyFunction/isActive}}checked{{/if}} data-uk-modal="{target:'#toggle-alert-modal'}"></span></td>
                <td><span class="init-update-function-btn uk-button" data-row-id="{{anomalyFunctionIndex}}" data-uk-modal="{target:'#update-function-modal'}" data-uk-tooltip title="Edit"><i class="uk-icon-pencil"></i></span></td>
                <td><span class="init-delete-anomaly-function uk-button" data-function-id="{{anomalyFunction/id}}" data-function-name="{{anomalyFunction/functionName}}" data-uk-modal="{target:'#delete-function-modal'}" data-uk-tooltip title="Delete"><i class="uk-icon-times"></i></span></td>
            </tr>
            {{/each}}
            </tbody>
        </table>
    </script>
</section>
