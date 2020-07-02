/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
var JS_BEAUTIFY_SETTINGS = {
  indent_size               : 4,
  indent_char               : ' ',
  preserve_newlines         : true,
  braces_on_own_line        : true,
  keep_array_indentation    : false,
  space_after_anon_function : true
};


$(document).ready(function() {
  EDITOR = CodeMirror.fromTextArea(document.getElementById('query-maker'), {
    lineWrapping: true,
    lineNumbers: true,
    mode: "sql",
    theme: "elegant"
  });

  RESULTS = CodeMirror.fromTextArea(document.getElementById('results-maker'), {
    mode         : 'ruby',
    value        : "",
    indentUnit   : 4,
    lineWrapping : true,
    lineNumbers  : false
  });

  var windowHeight = $(window).height();
  var documentHeight = $(document).height();

  var qH = (.20) * windowHeight + 10;
  var rH = (.60) * windowHeight;

  $('.query-box').css('min-height', qH + 'px');
  $('.query-results').css('min-height', rH + 'px');

  HELPERS.printTables(function() {
    $('.table-name-entry').click(function(e) {
      var tableName = e.currentTarget.attributes[1].nodeValue;
      HELPERS.decorateTableSchema(tableName, rH);
      EDITOR.setValue('');
      HELPERS.populateDefaultQuery(tableName);
      $("#execute-query").click();
    });
  }, qH);

  var table;

  $("#execute-query").click(function() {
    // execute query and draw the results
    var query = EDITOR.getValue().trim();
    var traceEnabled = document.getElementById('trace-enabled').checked;
    var querySyntaxPQL = document.getElementById('pql').checked;

    HELPERS.executeQuery(query, querySyntaxPQL, traceEnabled, function(data) {
      RESULTS.setValue(js_beautify(data, JS_BEAUTIFY_SETTINGS));

      var queryResponse = null;
      try {
        queryResponse = HELPERS.getAsObject(data);
      } catch (err) {
        if (table) {
          table.destroy();
          table = null;
        }

        HELPERS.resetResultsTable();
        return;
      }

      var dataArray = [];
      var columnList = [];
      if (querySyntaxPQL === true) {
        if (queryResponse) {
          if (queryResponse["selectionResults"]) {
            // Selection query
            columnList = _.map(queryResponse.selectionResults.columns, function (columnName) {
              return {"title": columnName};
            });
            dataArray = queryResponse.selectionResults.results;
          } else if (queryResponse["aggregationResults"] && queryResponse.aggregationResults.length > 0
              && !queryResponse.aggregationResults[0]["groupByResult"]) {
            // Simple aggregation query
            columnList = _.map(queryResponse.aggregationResults, function (aggregationResult) {
              return {"title": aggregationResult.function};
            });

            dataArray.push(_.map(queryResponse.aggregationResults, function (aggregationResult) {
              return aggregationResult.value;
            }));
          } else if (queryResponse["aggregationResults"] && queryResponse.aggregationResults.length > 0
              && queryResponse.aggregationResults[0]["groupByResult"]) {
            // Aggregation group by query
            var columns = queryResponse.aggregationResults[0].groupByColumns;
            columns.push(queryResponse.aggregationResults[0].function);
            columnList = _.map(columns, function (columnName) {
              return {"title": columnName};
            });

            dataArray = _.map(queryResponse.aggregationResults[0].groupByResult, function (aggregationGroup) {
              var row = aggregationGroup.group;
              row.push(aggregationGroup.value);
              return row;
            });
          }
        }
      } else {
        if (queryResponse["resultTable"] && queryResponse.resultTable["rows"]
          && queryResponse.resultTable.rows.length > 0) {
          columnList = _.map(queryResponse.resultTable.dataSchema.columnNames, function (columnName) {
            return {"title": columnName};
          });
          dataArray = queryResponse.resultTable.rows;
        }
      }
      HELPERS.resetResultsTable();

      table = $('#query-results-table').DataTable({
        destroy: true,
        data: dataArray,
        columns: columnList,
        scrollX: true
      });

      new $.fn.dataTable.Buttons(table, {
        buttons: [
          'copy', 'excel', 'csv'
        ]
      });

      table.buttons().container().prependTo(
        table.table().container()
      );
    })
  });
});

var HELPERS = {
  resetResultsTable: function() {
    $('#query-results-table-wrapper')
        .empty()
        .append("<table id=\"query-results-table\" class=\"display\" width=\"100%\"/></table>");
  },

  printTables : function(callback, qh) {
    $.get("/tables", function(data) {
      var source   = $("#table-names-template").html();
      var template = Handlebars.compile(source);
      var dataObj = HELPERS.getAsObject(data);
      var d = template(dataObj);
      $(".schema-list-view").html(d);
      $("#table-names").DataTable({
        "searching":true,
        "scrollY": qh + "px",
        "scrollCollapse": true,
        "paging": false,
        "info": false
      });
      callback();
    });
  },

  getAsObject : function(str) {
    if (typeof str === 'string' || str instanceof String) {
      return JSON.parse(str);
    }
    return str;
  },

  decorateTableSchema : function(tableName, th) {
    $.get("/tables/" + tableName + "/schema/", function(data){
      var schema = HELPERS.getAsObject(data);
      var source   = $("#table-schema-template").html();
      var template = Handlebars.compile(source);
      var d = template(schema);
      $(".schema-detail-view").html(d);
      $("#table-schema").DataTable({
        "searching":false,
        "scrollY": th + "px",
        "scrollCollapse": true,
        "paging": false,
        "info": false
      });
    }).fail(function() {
      $(".schema-detail-view").html("Table " + tableName + " schema not found")
    });
  },

  populateDefaultQuery: function(tableName) {
    EDITOR.setValue("select * from " + tableName + " limit 10");
  },

  populateQuery: function(query) {
    var query = EDITOR.getValue().trim();
  },

  executeQuery: function(query, querySyntaxPQL, traceEnabled, callback) {
    var url = undefined;
    var params = undefined;
    if (querySyntaxPQL === true) {
      url = "/pql";
      params = JSON.stringify({
        "pql": query,
        "trace": traceEnabled
      });
    } else {
      url = "/sql";
      params = JSON.stringify({
        "sql": query,
        "trace": traceEnabled
      });
    }
    $.ajax({
      type: 'POST',
      url: url,
      data: params,
      contentType: 'application/json; charset=utf-8',
      success: function (text) {
        callback(text);
      },
      dataType: 'text'
    })
    .complete(function(){
    });
  }
};
