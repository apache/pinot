<div class="analysis-card padding-all top-buffer">
  <h4 class="analysis-title bottom-buffer">[BETA] Root Cause Search</h4>
  <table class="table table-borderless rootcause__table">
    <thead>
    <tr>
      <td class="label-medium-semibold">Type</td>
      <td class="label-medium-semibold" style="max-width: 600px;">Details</td>
      <td class="label-medium-semibold">Score</td>
    </tr>
    </thead>
    <tbody>
    {{#each rootCauseData}}
      <tr>
        <td>{{type}}</td>
        <td style="max-width: 600px;"><a href="{{link}}" target="_blank">{{label}}</a></td>
        <td>{{score}}</td>
      </tr>
    {{/each}}
    </tbody>
  </table>
</div>
