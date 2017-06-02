import Ember from 'ember';

export function computeTextColor(params/*, hash*/) {
  const value = params[0];
  const opacity = Math.abs(value/25);
  if(opacity < 0.5){
    return "#000000";
  } else{
    return "#ffffff" ;
  }
}

export default Ember.Helper.helper(computeTextColor);
