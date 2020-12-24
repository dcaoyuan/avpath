import sbt.Keys._
import sbt._

object Dependencies {
  val SLF4J_VERSION = "1.7.24"

  val log = Seq(
    "org.slf4j" % "slf4j-api" % SLF4J_VERSION,
    "org.slf4j" % "jcl-over-slf4j" % SLF4J_VERSION,
    "org.slf4j" % "log4j-over-slf4j" % SLF4J_VERSION,
    "ch.qos.logback" % "logback-classic" % "1.1.2")

  val test = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0",
    "org.scalatest" %% "scalatest" % "3.0.8")

  val avro = Seq(
    "org.apache.avro" % "avro" % "1.8.2")

  val commonsCollections = "commons-collections" % "commons-collections" % "3.2.2"

  val jackson = Set(
    "org.codehaus.jackson" % "jackson-core-asl" % "1.9.15-TALEND",
    "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.15-TALEND"
  )

  val apacheCompress = "org.apache.commons" % "commons-compress" % "1.19"


  val basic: Seq[ModuleID] = log ++ test ++ avro

  val all = basic
}

object Formatting {

  import com.typesafe.sbt.SbtScalariform
  import com.typesafe.sbt.SbtScalariform.ScalariformKeys
  import ScalariformKeys._

  val BuildConfig = config("build") extend Compile
  val BuildSbtConfig = config("buildsbt") extend Compile

  // invoke: build:scalariformFormat
  val buildFileSettings: Seq[Setting[_]] = SbtScalariform.noConfigScalariformSettings ++
    inConfig(BuildConfig)(SbtScalariform.configScalariformSettings) ++
    inConfig(BuildSbtConfig)(SbtScalariform.configScalariformSettings) ++ Seq(
    scalaSource in BuildConfig := baseDirectory.value / "project",
    scalaSource in BuildSbtConfig := baseDirectory.value,
    includeFilter in(BuildConfig, format) := ("*.scala": FileFilter),
    includeFilter in(BuildSbtConfig, format) := ("*.sbt": FileFilter),
    format in BuildConfig := {
      val x = (format in BuildSbtConfig).value
      (format in BuildConfig).value
    },
    ScalariformKeys.preferences in BuildConfig := formattingPreferences,
    ScalariformKeys.preferences in BuildSbtConfig := formattingPreferences)

  val settings = SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := formattingPreferences,
    ScalariformKeys.preferences in Test := formattingPreferences)

  val formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(IndentSpaces, 2)
  }
}

object Packaging {

  import com.typesafe.sbt.SbtNativePackager._

  val settings = packagerSettings ++ deploymentSettings ++
    packageArchetype.java_application ++ Seq(
    name := "wandou-avpath",
    NativePackagerKeys.packageName := "wandou-avpath")
}

object CrossVersions {

  lazy val scala212 = "2.12.11"
  lazy val scala211 = "2.11.12"
  lazy val supportedScalaVersions = List(scala212, scala211)
  lazy val crossVersionSetting = crossScalaVersions := supportedScalaVersions

}

