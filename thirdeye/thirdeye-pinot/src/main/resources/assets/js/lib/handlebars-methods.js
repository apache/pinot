$(document).ready(function() {
   /** --- 1) Register Handelbars helpers --- * */
    
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
            return  Handlebars.helpers.assignColorByID(numIds,id)        }

    });

    Handlebars.registerHelper('colorByIdContributors', function( id, dimensionValuesMap, options ) {

        var numIds = dimensionValuesMap[options.hash.dimName].length

        if(parseInt(numIds) < 10){
            return d3.scale.category10().range()[id];

        } else if (parseInt(numIds) < 20) {
            return d3.scale.category20().range()[id];

        } else{
            return  Handlebars.helpers.assignColorByID(numIds,id)
        }
    });


    //If you change this method change the colorScale() and assignColorByID()functions too
    //those 2 are defining the color of the lines on every timeseries chart when the number of items are > 20
    Handlebars.registerHelper('assignColorByID', function(len, index){

        //16777216 = 256 ^ 3
        var diff = parseInt(16777216 / len);

        var diffAry = [];
        for (x=0; x<len; x++){
            diffAry.push(diff * x)
        }

        var colorAry = [];
        var num;
        for  (y=0; y<len; y++){

            if(y%2 == 0){
                num = diffAry[y/2]
            }else{
                num = diffAry[Math.floor(len - y/2)]
            }


            var str = (num.toString(16) + "dddddd")
            var hex = num.toString(16).length < 6 ? "#" + str.substr(0,6) : "#" + num.toString(16)
            colorAry.push( hex )
        }

        return colorAry[index]

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
            return  val + "%";
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

    //takes utc timestamp (milliseconds ie. 1462626000000), returns date and time in users tz
    Handlebars.registerHelper('millisToDate', function(millis) {

        if(!millis){
           return "n.a"
        }
        millis = parseInt(millis);
        var tz = getTimeZone();
        return moment(millis).tz(tz).format('YYYY-MM-DD h a z');
    });

    //takes utc timestamp (milliseconds ie. 1462626000000), returns date and time in users tz in a format in sync with the hash aggregate granularity
    Handlebars.registerHelper('millisToDateTimeInAggregate', function(millis) {

        if(!millis){
            return "n.a"
        }
        millis = parseInt(millis);
        var tz = getTimeZone();
        var dateTimeFormat = "h a";
        if(hash.hasOwnProperty("aggTimeGranularity") && hash.aggTimeGranularity == "DAYS"){

            dateTimeFormat = "MM-DD h a"
        }


        return moment(millis).tz(tz).format(dateTimeFormat);
    });

    Handlebars.registerHelper('parse', function(str, prop) {
        str = str.replace("/;/g", ',');
        var obj = JSON.parse(str);
        return obj[prop];
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

    /* var source_metric_time_series_section_anomaly = $("#metric-time-series-section-anomaly-template").html();
    HandleBarsTemplates.template_metric_time_series_section_anomaly = Handlebars.compile(source_metric_time_series_section_anomaly);*/
    
    var source_time_series_template =  $("#time-series-template").html();
    HandleBarsTemplates.template_time_series = Handlebars.compile(source_time_series_template);

    /*Hiding anomalies till it's ready for production
    var source_anomalies_template = $("#anomalies-template").html();
    HandleBarsTemplates.template_anomalies = Handlebars.compile(source_anomalies_template);*/




})
