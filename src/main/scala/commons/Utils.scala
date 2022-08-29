package commons

object Utils {

  implicit class MapOps(m: Map[String, String]) {
    def toJavaProperties: java.util.Properties =
      m.foldLeft(new java.util.Properties()) { case (props, (k, v)) =>
        props.put(k, v)
        props
      }
  }
}
