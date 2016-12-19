<div class="col-md-12">
  <div class="table-responsive">
    <table class="table table-bordered analysis-table">
      <thead>
      <tr>
        {{#each this.timeBucketsCurrent as |timestamp timeIndex|}}
        <th>{{timestamp}}</th>
        {{/each}}
      </tr>
      </thead>
      <tbody>
      {{#each this as |key keyIndex|}}
      <tr>
        <td>{{keyIndex}}</td>
      </tr>
      {{/each}}
      </tbody>
    </table>
  </div>
</div>
