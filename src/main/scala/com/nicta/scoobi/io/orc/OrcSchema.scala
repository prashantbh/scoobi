package com.nicta.scoobi.io.orc


import org.apache.hadoop.hive.ql.io.orc.OrcStruct

/** Type class for conversions between basic Scala types and Hadoop Writable types. */
trait OrcSchema[A] {
  def toWritable(x: A): OrcStruct
  def fromWritable(x: OrcStruct): A
  val mf: Manifest[OrcStruct]
}