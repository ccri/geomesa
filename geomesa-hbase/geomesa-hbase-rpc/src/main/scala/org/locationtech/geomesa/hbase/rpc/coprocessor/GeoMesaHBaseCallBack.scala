/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.coprocessor

import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue}

import com.google.protobuf.ByteString
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorResponse

class GeoMesaHBaseCallBack extends Callback[GeoMesaCoprocessorResponse] {

  var isDone = false
  var lastRow: Array[Byte] = _

  val result = new LinkedBlockingQueue[ByteString]()

  override def update(region: Array[Byte], row: Array[Byte], response: GeoMesaCoprocessorResponse): Unit = {
    isDone = true

    val result =  Option(response).map(_.getPayloadList).orNull
    val lastscanned: ByteString = Option(response).map(_.getLastscanned).orNull

    if (lastscanned != null && !lastscanned.isEmpty) {
      println(s"Last Scanned is not null and is not Empty with length ${lastRow.length}: ${lastscanned}")
      lastRow = lastscanned.toByteArray
      isDone = false
    } else {
      //println("Last scanned is null")
    }

    if (result != null) {
      println(s"In update for region ${region} row: $row")
      this.result.addAll(result)

    }
  }


}
