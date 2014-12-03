package com.nicta.scoobi
package io
package orc

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.core.Source
import com.nicta.scoobi.core.InputConverter
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import com.nicta.scoobi.core.DataSource
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import com.nicta.scoobi.impl.plan.DListImpl
import com.nicta.scoobi.impl.io.Files
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat

/** Type class for conversions between basic Scala types and Hadoop Writable types. */
trait OrcSchema[A] {
  def toWritable(x: A): OrcStruct
  def fromWritable(x: OrcStruct): A
  val mf: Manifest[OrcStruct]
}

trait OrcInput {
  def fromOrcFile[V: WireFormat: OrcSchema](paths: String*): DList[V] =
    fromOrcFile(paths, checkValueType = true)

  /**
   * Create a new DList from the "value" contents of a list of one or more Orc Files. Note that the type parameter
   * V is the "converted" Scala type for the Writable value type that must be contained in the the
   * Orc Files. In the case of a directory being specified, the input forms all the files in that directory.
   */
  def fromOrcFile[V: WireFormat: OrcSchema](paths: Seq[String], checkValueType: Boolean = true, check: Source.InputCheck = Source.defaultInputCheck): DList[V] = {
    val convV = implicitly[OrcSchema[V]]

    val converter = new InputConverter[NullWritable, OrcStruct, V] {
      def fromKeyValue(context: InputContext, k: NullWritable, v: OrcStruct) = convV.fromWritable(v)
    }

    fromOrcSource(new OrcSource[V](paths, converter))
  }

  def fromOrcSource[A: WireFormat](source: OrcSource[A]) = DListImpl[A](source)
}

object OrcInput extends OrcInput

/* Class that abstracts all the common functionality of reading from orc files. */
class OrcSource[A: WireFormat](paths: Seq[String],
  val inputConverter: InputConverter[NullWritable, OrcStruct, A],
  checkFileTypes: Boolean = true,
  val check: Source.InputCheck = Source.defaultInputCheck)
  extends DataSource[NullWritable, OrcStruct, A] {
  val inputFormat = classOf[OrcNewInputFormat]

  private val inputPaths = paths.map(p => new Path(p))

  override def toString = "OrcSource(" + id + ")" + inputPaths.mkString("\n", "\n", "\n")

  /**
   * Check if the input path exists, and optionally the expected key/value types match those in the file.
   * For efficiency, the type checking will only check one file per dir
   */
  def inputCheck(implicit sc: ScoobiConfiguration) {
    check(inputPaths, sc)
    inputPaths foreach checkInputPathType
  }

  protected def checkInputPathType(p: Path)(implicit sc: ScoobiConfiguration) {}

  def inputConfigure(job: Job)(implicit sc: ScoobiConfiguration) {
    inputPaths foreach { p => FileInputFormat.addInputPath(job, p) }
  }

  def inputSize(implicit sc: ScoobiConfiguration): Long = inputPaths.map(p => Files.pathSize(p)(sc)).sum
}

