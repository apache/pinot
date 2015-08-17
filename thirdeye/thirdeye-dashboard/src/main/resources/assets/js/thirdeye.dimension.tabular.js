$(document).ready(function() {
    $("#intra-day-update").click(function() {
      $(".dimension-tabular-table").each(function(i, elt) {
        updateTableOrder($(elt), $("#intra-day-table-order"))
      })
    })

    // If order is already defined, set UI component and update
    var hashParams = parseHashParameters(window.location.hash)
    var existingOrder = []
    $.each(hashParams, function(key, val) {
      if (key.indexOf('intraDayOrder_') == 0) {
        var idx = parseInt(key.split('_')[1])
        existingOrder[idx] = val
      }
    })

    if (existingOrder.length > 0) {
      $("#intra-day-table-order li div").each(function(i, elt) {
        $(elt).html(existingOrder[i])
      })
      $(".dimension-tabular-table").each(function(i, elt) {
        updateTableOrder($(elt), $("#intra-day-table-order"))
      })
    }
})
