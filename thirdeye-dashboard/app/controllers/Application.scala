package controllers

import play.api._
import play.api.mvc._
import play.api.Play.current
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

object Application extends Controller {

  def index = Action {
    Ok(views.html.index())
  }

  def heatmap(collection: String, metric: String, dimension: String, baseline: Long, current: Long) = Action {
    Ok(views.html.heatmap(collection, metric, dimension, baseline, current))
  }

}