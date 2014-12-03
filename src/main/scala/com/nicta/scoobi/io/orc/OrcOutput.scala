package com.nicta.scoobi
package io
package orc

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, RecordWriter, Job }
import core._
import impl.io.Files
import org.apache.hadoop.conf.Configuration
import impl.ScoobiConfigurationImpl
import org.apache.hadoop.io.compress._
import java.util.zip.Deflater
import org.apache.hadoop.io.SequenceFile.CompressionType
import com.nicta.scoobi.impl.util.Compatibility
import java.io.OutputStream
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat
import com.nicta.scoobi.core.DataSource
import org.apache.hadoop.hive.ql.io.orc.OrcSerde
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/** Functions for persisting distributed lists by storing them as ORC files. */
trait OrcOutput {

  def orcSink[B](path: String, typeString:String, overwrite: Boolean = false, check: Sink.OutputCheck = Sink.defaultOutputCheck)(implicit sc: ScoobiConfiguration) = {

    val converter = new OutputConverter[NullWritable, Writable, B] {

      def toKeyValue(x: B)(implicit configuration: Configuration) = {
        val serde = new OrcSerde
        val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
        val oip: ObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
        val row = serde.serialize(x, oip)
        (NullWritable.get, row)
      }
    }

    OrcSink[Writable, B](path, converter, overwrite, check)
  }

}

object OrcOutput extends OrcOutput

case class OrcSink[V, B](path: String,
  outputConverter: OutputConverter[NullWritable, Writable, B],
  overwrite: Boolean = false,
  check: Sink.OutputCheck = Sink.defaultOutputCheck,
  compression: Option[Compression] = None) extends DataSink[NullWritable, Writable, B] /*with SinkSource*/ {

  private implicit lazy val logger = LogFactory.getLog("scoobi.OrcOutput")

  lazy val output = new Path(path)

  override def outputFormat(implicit sc: ScoobiConfiguration) = classOf[FileOutputFormat[NullWritable, Writable]]

  def outputKeyClass(implicit sc: ScoobiConfiguration) = classOf[NullWritable]
  def outputValueClass(implicit sc: ScoobiConfiguration) = classOf[Writable]

  def outputCheck(implicit sc: ScoobiConfiguration) {
    check(output, overwrite, sc)
  }
  def outputPath(implicit sc: ScoobiConfiguration) = Some(output)

  def outputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    FileOutputFormat.setOutputPath(job, output)
  }
  
  /*def toSource: Source = {
    val converter = new InputConverter[NullWritable, OrcStruct, B]{
      val convV = implicitly[OrcSchema[B]]
      def fromKeyValue(context: InputContext, k: NullWritable, v: OrcStruct) = convV.fromWritable(V)
    }
    OrcInput.fromOrcSource(new OrcSource[B](Seq(path), converter))
  }*/
  
  override def outputSetup(implicit sc: ScoobiConfiguration) {
    super.outputSetup(sc)
    if (Files.pathExists(output)(sc.configuration) && overwrite) {
      logger.info("Deleting the pre-existing output path: " + output.toUri.toASCIIString)
      Files.deletePath(output)(sc.configuration)
    }
  }

  def compressWith(codec: CompressionCodec, compressionType: CompressionType = CompressionType.BLOCK) = copy(compression = Some(Compression(codec, compressionType)))

  override def toString = s"${getClass.getSimpleName}: ${outputPath(new ScoobiConfigurationImpl).getOrElse("none")}"
}
