<div class="analysis-card padding-all top-buffer">
  <h4 class="analysis-title bottom-buffer">[BETA] Root Cause Search</h4>
  <ul class="nav nav-tabs" role="tablist">
    <li><a class="active" data-toggle="tab" data-selector="">All</a></li>
    <li><a data-toggle="tab" data-selector="SERVICE">Services</a></li>
    <li><a data-toggle="tab" data-selector="HOLIDAY">Holidays</a></li>
  </ul>
  <div class="tab-content">
    <div class="tab-pane active" id="tab-table1">
      <table id="rootcause-data-table" class="table table-borderless rootcause__table"></table>
    </div>
  </div>
</div>

<script type="application/javascript">
  $(document).ready(function() {
    $('a[data-toggle="tab"]').on('click', function (e) {
      var selector = $(this).data('selector');
      $('#rootcause-data-table').DataTable().columns( 0 ).search( selector ).draw();
    });
  });
</script>
