import coursier.maven.MavenRepository
import mill._, scalalib._, scalalib.publish._, scalafmt._
import mill.define.Cross.Resolver
import mill.scalalib.Assembly._
import $ivy.`com.lihaoyi::mill-contrib-buildinfo:`
import mill.contrib.buildinfo.BuildInfo
import $ivy.`io.chris-kipp::mill-ci-release::0.1.9`
import io.kipp.mill.ci.release.CiReleaseModule

val url = "https://github.com/nightscape/dataframe-io"

trait DfioModule extends CrossScalaModule with Cross.Module2[String, String] with SbtModule with CiReleaseModule with ScalafmtModule {
  val sparkVersion = crossValue2
  val Array(sparkMajor, sparkMinor, sparkPatch) = sparkVersion.split("\\.")
  val sparkBinaryVersion = s"$sparkMajor.$sparkMinor"
  override def artifactNameParts: T[Seq[String]] =
    Seq(super.artifactNameParts().head, "spark", sparkBinaryVersion)

  def sonatypeUri: String = sys.env.getOrElse("SONATYPE_URL", "https://define.it.via.env/SONATYPE_URL")
  def sonatypeSnapshotUri: String = sys.env.getOrElse("SONATYPE_SNAPSHOT_URL", "https://define.it.via.env/SONATYPE_SNAPSHOT_URL")
  override def stagingRelease = false

  def compileIvyDeps = Agg(
    ivy"org.apache.spark::spark-core:$sparkVersion",
    ivy"org.apache.spark::spark-sql:$sparkVersion"
  )
  def pomSettings = PomSettings(
    description = "A Spark library for reading and writing from/to various data sources",
    organization = "dev.mauch.spark.dfio",
    url = url,
    licenses = Seq(License.MIT),
    versionControl = VersionControl(
      browsableRepository = Some(url),
      connection = Some(s"scm:git:$url.git"),
    ),
    developers = Seq(Developer("nightscape", "Martin Mauch", "https://github.com/nightscape")),
  )
  override implicit def crossSbtModuleResolver: Resolver[CrossModuleBase] = new Resolver[CrossModuleBase] {
    def resolve[V <: CrossModuleBase](c: Cross[V]): V = c.valuesToModules(List(crossValue, crossValue2))
  }
}

trait HwcModule extends SbtModule {
  def repositoriesTask = T.task {
    super.repositoriesTask() ++ Seq(MavenRepository("https://repository.cloudera.com/artifactory/libs-release-local/"))
  }
}

trait ConfluentRepo extends ScalaModule {
  def repositoriesTask = T.task {
    super.repositoriesTask() ++ Seq(MavenRepository("https://packages.confluent.io/maven/"))
  }
}
val scala213 = "2.13.13"
val scala212 = "2.12.19"
val spark24 = List("2.4.8")
val spark30 = List("3.0.3")
val spark31 = List("3.1.3")
val spark32 = List("3.2.4")
val spark33 = List("3.3.4")
val spark34 = List("3.4.2")
val spark35 = List("3.5.1")
val sparkVersions = spark24 ++ spark30 ++ spark31 ++ spark32 ++ spark33 ++ spark34 ++ spark35
val crossMatrix212 = sparkVersions.map(spark => (scala212, spark))
val crossMatrix213 = sparkVersions.filter(_ >= "3.2").map(spark => (scala213, spark))
val crossMatrix = crossMatrix212 ++ crossMatrix213


object core extends Cross[DfioModule](crossMatrix)

trait SerdeModule extends DfioModule with ConfluentRepo {
  def moduleDeps = Seq(core())
  def ivyDeps = Agg(
    ivy"org.scala-lang.modules::scala-xml:1.3.0",
    ivy"com.fasterxml.jackson.module::jackson-module-scala:2.13.2",
    ivy"za.co.absa::abris:6.4.0"
  )
}
object serde extends Cross[SerdeModule](crossMatrix)

trait HiveModule extends DfioModule with HwcModule {
  def moduleDeps = Seq(core())
  def compileIvyDeps = Agg(
    ivy"com.hortonworks.hive::hive-warehouse-connector-spark3:1.0.0.7.2.17.0-334"
      .exclude("net.minidev"-> "json-smart")
      .exclude("net.shibboleth.tool"-> "xmlsectool")
      .exclude("org.cloudera.logredactor"-> "logredactor")
      .excludeOrg("androidx.annotation")
  )
}
object hive extends Cross[HiveModule](crossMatrix212)

trait ExcelModule extends DfioModule {
  def moduleDeps = Seq(core())
  def ivyDeps = Agg(ivy"com.crealytics::spark-excel:3.3.3_0.20.3")
}
object excel extends Cross[ExcelModule](crossMatrix)

trait KafkaModule extends DfioModule with ConfluentRepo {
  def moduleDeps = Seq(core(), serde())
  def ivyDeps = Agg(ivy"org.apache.spark::spark-sql-kafka-0-10:$sparkVersion")
}
object kafka extends Cross[KafkaModule](crossMatrix)

trait SolrModule extends DfioModule {
  def moduleDeps = Seq(core())
  def ivyDeps = Agg(
    ivy"org.apache.solr:solr-solrj:9.5.0",
    ivy"commons-codec:commons-codec:1.15"
  )
}
object solr extends Cross[SolrModule](crossMatrix)

trait UriParserModule extends DfioModule with HwcModule with ConfluentRepo {
  val hiveModuleDep = if (crossValue < "2.13") Seq(hive()) else Seq()
  def moduleDeps = Seq(core(), serde(), excel(), kafka(), solr()) ++ hiveModuleDep
  def scalacOptions = Seq("-Xexperimental")
}
object `uri-parser` extends Cross[UriParserModule](crossMatrix)

trait EtlModule extends DfioModule with HwcModule with ConfluentRepo with BuildInfo {
  //def mainClass = Some("dev.mauch.spark.dfio.ETL")
  //def finalMainClassOpt = Left[String, String]("none")
  val hiveModuleDep = if (crossValue < "2.13") Seq(hive()) else Seq()
  def moduleDeps = Seq(core(), serde(), excel(), kafka(), solr(), `uri-parser`()) ++ hiveModuleDep
  //def artifactName = "etl"
  def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:$sparkVersion",
    ivy"com.lihaoyi::mainargs:0.6.3",
  ).map(_.excludeOrg("androidx.annotation"))
  def assemblyRules = super.assemblyRules ++ Seq(
    Rule.AppendPattern("META-INF/services/.*"),
    Rule.Append("application.conf"), // all application.conf files will be concatenated into single file
    Rule.AppendPattern(".*\\.conf"), // all *.conf files will be concatenated into single file
    Rule.ExcludePattern(".*.temp"), // all *.temp files will be excluded from a final jar
    Rule.ExcludePattern("scala/.*"),
    Rule.ExcludePattern("org\\.apache\\.spark/.*"),
  )
  def buildInfoPackageName = "dev.mauch.spark.dfio"
  def buildInfoMembers =
    Seq(
      BuildInfo.Value("version", publishVersion()),
      BuildInfo.Value("formattedShaVersion", "TODO"),
      BuildInfo.Value("scalaVersion", scalaVersion()),
      BuildInfo.Value("sparkVersion", sparkVersion),
    )
}
object etl extends Cross[EtlModule](crossMatrix)
