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

  def metricRatio(collection: String,
                  metricName: String,
                  baselineTime: Long,
                  currentTime: Long,
                  timeWindow: Integer) = Action.async { implicit request =>

    val adjustedBaselineTime = (baselineTime / timeWindow) * timeWindow
    val adjustedCurrentTime = (currentTime / timeWindow) * timeWindow

    val baselineUrl = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/metrics/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(adjustedBaselineTime)
      .append("/")
      .append(adjustedBaselineTime + timeWindow - 1)
      .append("?")

    val currentUrl = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/metrics/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(adjustedCurrentTime)
      .append("/")
      .append(adjustedCurrentTime + timeWindow - 1)
      .append("?")

    addDimensionValues(baselineUrl, request.queryString)
    addDimensionValues(currentUrl, request.queryString)

    for {
      baselineMetrics <- WS.url(baselineUrl.toString()).get()
      currentMetrics <- WS.url(currentUrl.toString()).get()
    } yield {
      val baseline = (baselineMetrics.json.apply(0) \ "metricValues" \ metricName).as[Double]
      val current = (currentMetrics.json.apply(0) \ "metricValues" \ metricName).as[Double]

      val result = Json.obj(
        "name" -> metricName,
        "ratio" -> JsNumber((current - baseline) / baseline)
      )

      Ok(result)
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

    val adjustedBaselineTime = (baselineTime / timeWindow) * timeWindow
    val adjustedCurrentTime = (currentTime / timeWindow) * timeWindow

    val baselineUrl = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/metrics/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(adjustedBaselineTime)
      .append("/")
      .append(adjustedBaselineTime + timeWindow - 1)
      .append("?")

    val currentUrl = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/metrics/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(adjustedCurrentTime)
      .append("/")
      .append(adjustedCurrentTime + timeWindow - 1)
      .append("?")

    val dimensionValues = request.queryString + (dimensionName -> Seq("!"))

    addDimensionValues(baselineUrl, dimensionValues)
    addDimensionValues(currentUrl, dimensionValues)

    for {
      baselineMetrics <- WS.url(baselineUrl.toString()).get()
      currentMetrics <- WS.url(currentUrl.toString()).get()
    } yield {
      val baseline = getDimensionMetricMapping(baselineMetrics.json, metricName, dimensionName)
      val current = getDimensionMetricMapping(currentMetrics.json, metricName, dimensionName)
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
                 timeWindow: Integer,
                 normalized: Boolean) = Action.async { implicit request =>

    val adjustedBaselineTime = (baselineTime / timeWindow) * timeWindow
    val adjustedCurrentTime = (currentTime / timeWindow) * timeWindow

    val url = new StringBuilder()
      .append(play.Configuration.root().getString("thirdeye.url"))
      .append("/timeSeries/")
      .append(URLEncoder.encode(collection, "UTF-8"))
      .append("/")
      .append(URLEncoder.encode(metricName, "UTF-8"))
      .append("/")
      .append(adjustedBaselineTime)
      .append("/")
      .append(adjustedCurrentTime + timeWindow - 1)

    url.append("?")

    addDimensionValues(url, request.queryString)

    WS.url(url.toString()).get().map { response =>

      val result = Json.toJson(response.json.as[Seq[JsObject]].map(datum => {
        var data = (datum \ "data")
          .as[Seq[JsValue]]
          .map(point => ((point.apply(0).as[Long] / timeWindow) * timeWindow, point.apply(1).as[Double]))
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
          .sortBy(x => x._1)

        if (normalized) {
          val baselineValue = data.head._2
          data = data.map(point => (point._1, point._2 / baselineValue))
        }

        Json.obj(
          "label" -> datum \ "label",
          "dimensionValues" -> datum \ "dimensionValues",
          "data" -> data.map(point => Json.arr(JsNumber(point._1), JsNumber(point._2)))
        )
      }))

      Ok(result)
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
