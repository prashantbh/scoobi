/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi.io

import com.nicta.scoobi.Scoobi._

package object thrift {

  type ThriftLike = org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]

  implicit def ThriftWireFormat[A](implicit m: Manifest[A], ev: A <:< ThriftLike): WireFormat[A] =ThriftSchema.mkThriftFmt[A]

  implicit def ThriftSeqSchema[A](implicit m: Manifest[A], ev: A <:< ThriftLike): SeqSchema[A] = ThriftSchema.mkThriftSchema[A]
}