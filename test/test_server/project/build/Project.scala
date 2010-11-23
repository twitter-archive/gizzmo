import sbt._
import com.twitter.sbt.StandardProject


class GizzmoServerProject(info: ProjectInfo) extends StandardProject(info) {
  override def compileOptions = super.compileOptions ++ Seq(Unchecked)
  override def filterScalaJars = false

  val scalaTools = "org.scala-lang" % "scala-compiler" % "2.7.7"
  val gizzard    = "com.twitter" % "gizzard" % "1.5.4-mc-SNAPSHOT"

  val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"
}
