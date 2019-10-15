import sbt._
import sbt.Keys._
import sbtfilter.Plugin.FilterKeys._
import scoverage.ScoverageSbtPlugin._

object Build extends sbt.Build {

  lazy val avpath = Project("wandou-avpath", file("."))
    .settings(basicSettings: _*)
    .settings(Formatting.settings: _*)
    .settings(Formatting.buildFileSettings: _*)
    .settings(releaseSettings: _*)
    .settings(sbtrelease.ReleasePlugin.releaseSettings: _*)
    .settings(libraryDependencies ++= Dependencies.avro ++ Dependencies.test)
    .settings(Packaging.settings: _*)
    .settings(sbtavro.SbtAvro.avroSettings ++ avroSettingsTest: _*)
    .settings(instrumentSettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)

  lazy val basicSettings = Seq(
    organization := "com.wandoulabs.avro",
    version := "0.1.4-talend",
    scalaVersion := "2.11.12",
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    resolvers ++= Seq(
      "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Typesafe repo" at "http://repo.typesafe.com/typesafe/releases/"),
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"))

  lazy val avroSettings = Seq(
    sbtavro.SbtAvro.stringType in sbtavro.SbtAvro.avroConfig := "String",
    sourceDirectory in sbtavro.SbtAvro.avroConfig <<= (resourceDirectory in Compile)(_ / "avsc"),
    version in sbtavro.SbtAvro.avroConfig := "1.8.2")

  // Todo rewrite sbt-avro to compile in Test phase.
  lazy val avroSettingsTest = Seq(
    sbtavro.SbtAvro.stringType in sbtavro.SbtAvro.avroConfig := "String",
    sourceDirectory in sbtavro.SbtAvro.avroConfig <<= (resourceDirectory in Test)(_ / "avsc"),
    javaSource in sbtavro.SbtAvro.avroConfig <<= (sourceManaged in Test)(_ / "java" / "compiled_avro"),
    version in sbtavro.SbtAvro.avroConfig := "1.8.2")

  lazy val releaseSettings = Seq(
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    publishTo := {
      if (isSnapshot.value)
        Some("talend_nexus_deployment" at "https://artifacts-oss.talend.com/nexus/content/repositories/TalendOpenSourceSnapshot")
      else
        Some("talend_nexus_deployment"  at "https://artifacts-oss.talend.com/nexus/content/repositories/TalendOpenSourceRelease")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { (repo: MavenRepository) => false },
    pomExtra := pomXml)

  lazy val pomXml =
    (<url>https://github.com/wandoulabs/avpath</url>
     <licenses>
       <license>
         <name>Apache License 2.0</name>
         <url>http://www.apache.org/licenses/</url>
         <distribution>repo</distribution>
       </license>
     </licenses>
     <scm>
       <url>git@github.com:talend/avpath.git</url>
       <connection>scm:git:git@github.com:talend/avpath.git</connection>
     </scm>)

  lazy val noPublishing = Seq(
    publish := (),
    publishLocal := (),
    // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
    // https://github.com/sbt/sbt-pgp/issues/36
    publishTo := None
  )

}

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
      includeFilter in (BuildConfig, format) := ("*.scala": FileFilter),
      includeFilter in (BuildSbtConfig, format) := ("*.sbt": FileFilter),
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
  import com.typesafe.sbt.packager.Keys._
  import com.typesafe.sbt.packager.archetypes._

  val settings = packagerSettings ++ deploymentSettings ++
    packageArchetype.java_application ++ Seq(
      name := "wandou-avpath",
      NativePackagerKeys.packageName := "wandou-avpath")
}

