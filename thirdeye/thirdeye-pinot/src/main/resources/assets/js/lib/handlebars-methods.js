$(document).ready(function() {
   /** --- 1) Register Handelbars helpers --- * */

   //takes a string returns a HEX color code
    Handlebars.registerHelper('colorByName', function(name) {

        if(name == "?"){
            name = "other"
        }else if(name == ""){
            name = "unknown"
        }
        //remove non alphanumeric characters
        name = name.replace(/\W+/g, "")

        //multiple short name would
        if(name.length < 4){
            name = name + name + name;
            name=  name.substr(-2) + name.substr(-2) + name.substr(-2);
        }

        //too long name would return infinity
        var hexStr;
        if (Number.isFinite(parseInt(name, 36) + 16777216)){
            hexStr =  (parseInt(name, 36) + 16777216).toString(16).substr(0,6);
        }else{
            name =  name.substr(-2) + name.substr(-2) + name.substr(-2);
            hexStr =  (parseInt(name, 36) + 16777216).toString(16).substr(0,6);
        }

        var color = "#" + hexStr

        return color

    });
    
  //takes a string returns a HEX color code
    Handlebars.registerHelper('colorById', function( id, numIds, options ) {

        if(typeof numIds == "object"){
            var keysAry = Object.keys(numIds);
            numIds = keysAry.length;
        }

        if(parseInt(numIds) < 10){
            return d3.scale.category10().range()[id];

        } else if (parseInt(numIds) < 20) {
            return d3.scale.category20().range()[id];

        } else{
            return  Handlebars.helpers.colorByName( options.hash.name )
        }
    });

    Handlebars.registerHelper('colorByIdContributors', function( id, dimensionValuesMap, options ) {

        var numIds = dimensionValuesMap[options.hash.dimName].length

        if(parseInt(numIds) < 10){
            return d3.scale.category10().range()[id];

        } else if (parseInt(numIds) < 20) {
            return d3.scale.category20().range()[id];

        } else{
            return  Handlebars.helpers.colorByName( options.hash.name )
        }
    });



    Handlebars.registerHelper('hide_if_eq', function(param1, param2){
       if(param1 == param2){
           return "uk-hidden"
       }
    });



        //takes a value and if alias is available displays the alias
    //in dashboards the derived metrics can have an alias int he configuration
    Handlebars.registerHelper('displayAlias', function(name, alias) {

        if(alias != undefined && alias.length > 0){
            return alias
        }else{
            return name
        }
    });

    Handlebars.registerHelper('displayRatio', function (val, index) {

        if ((index + 1) % 3 == 0 && parseFloat(val)) {
            // Round the ratio to 2 decimal places, add 0.00001 to prevent
            // Chrome rounding 0.005 to 0.00
            var ratioVal = Math.round((parseFloat(val) + 0.00001)).toFixed(1)
            val = ratioVal + "%"
            return val
        } else {
            return val.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        }
    });

    //Todo: remove this helper from the methods and from the template once the tabular view JSON is passing formatted data
    Handlebars.registerHelper('displayRatioUnformattedData', function (val, index) {

        if ((index + 1) % 3 == 0 && parseFloat(val)) {
            // Round the ratio to 2 decimal places, add 0.00001 to prevent
			// Chrome rounding 0.005 to 0.00
            var ratioVal = Math.round(((parseFloat(val) + 0.00001) * 1000) / 10).toFixed(1)
            val = ratioVal + "%"
            return val
        } else {
            return val.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        }
    });

    // If dimension value is null or "?" replace it with unknown or other
    Handlebars.registerHelper('displayDimensionValue', function(dimensionValue) {

        if (dimensionValue == ""){
            return "UNKNOWN";
        }else if(dimensionValue == "?"){
            return "OTHER";
        }else {
            return dimensionValue;
        }
    });

    //takes utc date iso format ie. 2016-04-06T07:00:00.000Z, returns date and time in users tz
    Handlebars.registerHelper('displayDate', function(date) {
        var tz = getTimeZone();
        return moment(date).tz(tz).format('YYYY-MM-DD h a z');
    });

    /* Add details-cell or heatmap-cell class to cells */
    Handlebars.registerHelper('classify', function (index) {
        if ((index + 1) % 3 == 0) {
            className = "heat-map-cell"
        } else if ((index + 1) % 3 == 1) {
            className = "details-cell border-left hidden"
        } else {
            className = "details-cell hidden"
        }
        return  className
    });

    //to get dimensionValue data for timebuckets ary in contributors ajax response
    Handlebars.registerHelper('returnValue', function(obj, options) {
    	var responseData = obj.responseData;
        var schema = obj.schema.columnsToIndexMapping;
        var rowId = options.hash.key;
        var schemaItem = options.hash.schemaItem;
        var indexFordisplayRatioHelper = 0;
        if(schemaItem == "percentageChange"){
            indexFordisplayRatioHelper = 2;
        }

        return Handlebars.helpers.displayRatio(responseData[rowId][schema[schemaItem]], indexFordisplayRatioHelper )
    })

    //takes an object and a key as option param and returns an object as a scope
    Handlebars.registerHelper('lookupDimValues', function(obj, options) {
        //Without the options.fn()  the raw object would be returned to be the html content
        return options.fn(obj[options.hash.dimName])
    });
    
    //takes an object and a key as option param and returns an object as a scope
    Handlebars.registerHelper('lookupRowIdList', function(obj, options) {
        //Without the options.fn()  the raw object would be returned to be the html content
        return options.fn(obj[options.hash.metricName + '|' + options.hash.dimName + '|' + options.hash.dimValue ])
    });


    //if param 1 == param 2
    Handlebars.registerHelper('if_eq', function(a, b, opts) {
        if(a == b){ // Or === depending on your needs
            return opts.fn(this);
        }
        return opts.inverse(this);
    });

    HandleBarsTemplates = {

    }
    /** --- 2) Create Handelbars templating method --- * */

    var source_tab_template =  $("#tab-template").html();
    HandleBarsTemplates.template_tab = Handlebars.compile(source_tab_template);

    var source_form_template =  $("#form-template").html();
    HandleBarsTemplates.template_form = Handlebars.compile(source_form_template);

    var source_datasets_template = $("#datasets-template").html();
    HandleBarsTemplates.template_datasets = Handlebars.compile(source_datasets_template);

    var source_filter_dimension_value_template =  $("#filter-dimension-value-template").html();
    HandleBarsTemplates.template_filter_dimension_value = Handlebars.compile(source_filter_dimension_value_template);

    var source_funnels_table = $("#funnels-table-template").html();
    HandleBarsTemplates.template_funnels_table = Handlebars.compile(source_funnels_table);

    var source_contributors_table = $("#contributors-table-template").html();
    HandleBarsTemplates.template_contributors_table = Handlebars.compile(source_contributors_table);


    var source_treemap_template = $("#treemap-template").html();
    HandleBarsTemplates.template_treemap = Handlebars.compile(source_treemap_template);

    var source_metric_time_series_section = $("#metric-time-series-section-template").html();
    HandleBarsTemplates.template_metric_time_series_section = Handlebars.compile(source_metric_time_series_section);
    
    var source_time_series_template =  $("#time-series-template").html();
    HandleBarsTemplates.template_time_series = Handlebars.compile(source_time_series_template);
})
