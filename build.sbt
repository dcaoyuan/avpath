import sbt.Keys._

scalaVersion := "2.12.11"

lazy val avpath = Project("wandou-avpath", file("."))
  .settings(basicSettings: _*)
  .settings(Formatting.settings: _*)
  .settings(Formatting.buildFileSettings: _*)
  .settings(releaseSettings: _*)
  .settings(sbtrelease.ReleasePlugin.releaseSettings: _*)
  .settings(libraryDependencies ++= Dependencies.avro ++ Dependencies.test)
  .settings(dependencyOverrides += Dependencies.commonsCollections)
  .settings(dependencyOverrides ++= Dependencies.jackson)
  .settings(dependencyOverrides += Dependencies.apacheCompress)
  .settings(Packaging.settings: _*)
  .settings(sbtavro.SbtAvro.avroSettings ++ avroSettingsTest: _*)
  .settings(CrossVersions.crossVersionSetting)

lazy val basicSettings = Seq(
  organization := "com.wandoulabs.avro",
  version := "0.1.7-talend",
  scalacOptions ++= Seq("-unchecked", "-deprecation"),
  resolvers ++= Seq(
    "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Typesafe repo" at "https://repo.typesafe.com/typesafe/releases/",
    "Talend Open Source Releases" at "https://artifacts-oss.talend.com/nexus/content/repositories/public/"
  ),
  javacOptions ++= Seq("-source", "1.6", "-target", "1.6"))

lazy val avroSettings = Seq(
  sbtavro.SbtAvro.stringType in sbtavro.SbtAvro.avroConfig := "String",
  sourceDirectory in sbtavro.SbtAvro.avroConfig <<= (resourceDirectory in Compile) (_ / "avsc"),
  version in sbtavro.SbtAvro.avroConfig := "1.8.2")

// Todo rewrite sbt-avro to compile in Test phase.
lazy val avroSettingsTest = Seq(
  sbtavro.SbtAvro.stringType in sbtavro.SbtAvro.avroConfig := "String",
  sourceDirectory in sbtavro.SbtAvro.avroConfig <<= (resourceDirectory in Test) (_ / "avsc"),
  javaSource in sbtavro.SbtAvro.avroConfig <<= (sourceManaged in Test) (_ / "java" / "compiled_avro"),
  version in sbtavro.SbtAvro.avroConfig := "1.8.2")

lazy val releaseSettings = Seq(
  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
  publishTo := {
    if (isSnapshot.value)
      Some("talend_nexus_deployment" at "https://artifacts-oss.talend.com/nexus/content/repositories/TalendOpenSourceSnapshot")
    else
      Some("talend_nexus_deployment" at "https://artifacts-oss.talend.com/nexus/content/repositories/TalendOpenSourceRelease")
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
