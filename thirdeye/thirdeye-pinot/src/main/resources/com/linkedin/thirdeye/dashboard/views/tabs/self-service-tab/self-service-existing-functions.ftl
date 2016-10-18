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
                <td style="font-size:1.2rem">{{anomalyFunction/functionName}}</td>
                <td style="font-size:1.2rem; font-weight: 700;">{{anomalyFunction/metric}}</td>
                <td>
                    <table>
                        {{#each anomalyFunction/properties as |propertyValue propertyKey|}}
                        <tr><td class="properties-cell">{{propertyKey}}</td><td class="properties-cell"><b>{{propertyValue}}</b></td></tr>
                        {{/each}}
                    </table>
                </td>

                <td>
                    {{#if anomalyFunction/filters}}
                    <table>
                        {{#each anomalyFunction/filters as |dimensionValues dimension|}}
                        <tr><td class="existing-anomaly-fn-filters-cell">{{dimension}}</td><td class="existing-anomaly-fn-filters-cell"><b>{{dimensionValues}}</b></td></tr>
                        {{/each}}
                    </table>
                    {{/if}}
                </td>
                <td><input class="init-toggle-active-state" type="checkbox" {{#if anomalyFunction/isActive}}checked{{/if}} data-row-id="{{anomalyFunctionIndex}}"  data-function-name="{{anomalyFunction/functionName}}" data-uk-modal="{target:'#toggle-alert-modal'}" data-uk-tooltip="" title="Turn on/off"></span></td>
                <td><span class="init-update-function-btn uk-button uk-button-large" data-row-id="{{anomalyFunctionIndex}}" data-uk-modal="{target:'#update-function-modal'}" data-uk-tooltip title="Edit"><i class="uk-icon-pencil"></i></span></td>
                <td><span class="init-delete-anomaly-function uk-button uk-button-large" data-function-id="{{anomalyFunction/id}}" data-function-name="{{anomalyFunction/functionName}}" data-uk-modal="{target:'#delete-function-modal'}" data-uk-tooltip title="Delete"><i class="uk-icon-times"></i></span></td>
            </tr>
            {{/each}}
            </tbody>
        </table>
        <div id="existing-fn-table-tooltip" class="hidden">
            <table>
            </table>
        </div>
    </script>
</section>
