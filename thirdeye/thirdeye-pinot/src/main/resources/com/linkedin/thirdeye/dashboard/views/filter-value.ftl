<section id="filter-dimension-value">
<script id="filter-dimension-value-template" type="text/x-handlebars-template">
   {{#each this}}
    <div class="value-filter" rel="{{@key}}" style="display: none;">
        <label style="display: block;"><input class="filter-select-all-checkbox" type="checkbox">Select All</label>
        <div class="filter-dimension-value-list uk-display-inline-block" style="width:250px;">
            {{#each this}}
            <label class="filter-dimension-value-item" rel="{{@../key}}" value="{{this}}">
                <input class="filter-value-checkbox" type="checkbox" rel="{{@../key}}" value="{{this}}"> {{displayDimensionValue this}}
            </label>
            {{/each}}
        </div>
    </div>
    {{/each}}
</script>
</section>

