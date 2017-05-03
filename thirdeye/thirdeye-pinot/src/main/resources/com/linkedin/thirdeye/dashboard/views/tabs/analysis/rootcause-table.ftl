<div class="analysis-card padding-all top-buffer">
  <h4 class="analysis-title bottom-buffer">[BETA] Root Cause Search
  <table class="table table-borderless rootcause__table">
    <thead>
    <tr>
      <td class="label-medium-semibold">Type</td>
      <td class="label-medium-semibold">Details</td>
      <td class="label-medium-semibold">Score</td>
    </tr>
    </thead>
    <tbody>
    {{#each rootCauseData}}
      <tr>
        <td>{{type}}</td>
        <td><a href="{{link}}">{{label}}</a></td>
        <td>{{score}}</td>
      </tr>
    {{/each}}
    </tbody>
  </table>
</div>
