package edu.hit.dblab.mctrix.matrix


import org.apache.spark.Partitioner


private[mctrix] class MatrixElemOpPartitioner(
    val numBlksByRow: Int,
    val numBlksByCol: Int) extends Partitioner {

  override def numPartitions: Int = numBlksByRow * numBlksByCol

  override def getPartition(key: Any): Int = {
    key match {
      case (blockId: BlockID) =>
        blockId.row * numBlksByCol + blockId.column
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case p: MatrixElemOpPartitioner =>
        (this.numBlksByRow == p.numBlksByRow) && (this.numBlksByCol == p.numBlksByCol)
      case _ =>
        false
    }
  }

}
