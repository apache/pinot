package controllers

import play.api._
import play.api.mvc._
import play.api.libs.ws.WS
import java.net.URLEncoder
import play.api.libs.json._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import play.api.Play.current

/**
 * Responsible for "/data" routes (which generate data consumable by UI)
 */
object Data extends Controller {

  /**
   * @return
   *   The dimension names / metric names for a collection
   */
  def config(collection: String) = Action.async { implicit request =>
    val url = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/collections/")
      .append(URLEncoder.encode(collection, "UTF-8"))

    WS.url(url.toString()).get().map { response =>
      Ok(response.json)
    }
  }

  /**
   * @return
   *   All collections registered with the server
   */
  def collections = Action.async { implicit request =>
    val url = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/collections")

    WS.url(url.toString()).get().map { response =>
      Ok(response.json)
    }
  }

  /**
   * @return
   *   A list of (currentMetric, baselineMetric, dimensionValue) tuples, sorted by currentMetric
   *   for all values of the given dimension.
   */
  def heatMap(collection: String,
              metricName: String,
              dimensionName: String,
              baselineTime: Long,
              currentTime: Long,
              timeWindow: Integer) = Action.async { implicit request =>

    val baselineUrl = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/metrics/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(baselineTime - timeWindow)
      .append("/")
      .append(baselineTime)
      .append("?")

    val currentUrl = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/metrics/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(currentTime - timeWindow)
      .append("/")
      .append(currentTime)
      .append("?")

    val dimensionValues = request.queryString + (dimensionName -> Seq("!"))

    addDimensionValues(baselineUrl, dimensionValues)
    addDimensionValues(currentUrl, dimensionValues)

    for {
      baselineMetrics <- WS.url(baselineUrl.toString()).get()
      currentMetrics <- WS.url(currentUrl.toString()).get()
    } yield {
      val baseline: Map[String, Long] = getDimensionMetricMapping(baselineMetrics.json, metricName, dimensionName)
      val current: Map[String, Long] = getDimensionMetricMapping(currentMetrics.json, metricName, dimensionName)
      val combined = current.map(e => Json.obj(
        "value" -> e._1,
        "baseline" -> baseline.getOrElse(e._1, 0).asInstanceOf[Long],
        "current" -> e._2)).toSeq.sortWith((x, y) => (x \ "current").as[Long] > (y \ "current").as[Long])
      Ok(Json.toJson(combined))
    }
  }

  def timeSeries(collection: String,
                 metricName: String,
                 baselineTime: Long,
                 currentTime: Long,
                 timeWindow: Integer) = Action.async { implicit request =>

    val url = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/timeSeries/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(URLEncoder.encode(metricName, "UTF-8"))
      .append("/")
      .append(baselineTime - timeWindow)
      .append("/")
      .append(currentTime)
      .append("?")

    addDimensionValues(url, request.queryString)

    WS.url(url.toString()).get().map { response =>
      Ok(response.json)
    }
  }

  private def addDimensionValues(url: StringBuilder, queryString: Map[String, Seq[String]]) = {
    for (query <- queryString) {
      for (value <- query._2) {
        url.append("&")
          .append(URLEncoder.encode(query._1, "UTF-8"))
          .append("=")
          .append(URLEncoder.encode(value, "UTF-8"))
      }
    }
  }

  private def getDimensionMetricMapping(data: JsValue,
                                        metricName: String,
                                        dimensionName: String): Map[String, Long] = {
    data.as[List[JsValue]]
        .map(e => (e \ "dimensionValues" \ dimensionName).as[String] -> (e \ "metricValues" \ metricName).as[Long])
        .toMap
  }

}
