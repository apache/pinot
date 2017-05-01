<div class="analysis-card padding-all top-buffer">
  <h4 class="analysis-title bottom-buffer">[BETA] Root Cause Search
    <div class="row">
      <div>
        <table class="table table-borderless rootcause__table">
          <thead>
          <tr>
            <td>Type</td>
            <td>Details</td>
            <td>Score</td>
          </tr>
          </thead>
          <tbody>
          {{#each rootCauseData}}
          <tr>
            <td>{{this.type}}</td>
            <td><a href="javascript:alert('{{this.link}}');">{{this.label}}</a></td>
            <td>{{this.score}}</td>
          </tr>
          {{/each}}
          </tbody>
        </table>
      </div>
    </div>
</div>
