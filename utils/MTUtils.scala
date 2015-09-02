package edu.hit.dblab.mctrix.utils

import java.nio.ByteBuffer

import edu.hit.dblab.mctrix.matrix._
import edu.hit.dblab.mctrix.vector.DistributedVector

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3
import scala.{specialized => spec}

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, min, max}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object MTUtils {

  /** Hash seeds to have 0/1 bits throughout. */
  private[mctrix] def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
    MurmurHash3.bytesHash(bytes)
  }

  /**
   * Function to generate a distributed BlockMatrix, in which every element is in the uniform distribution
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @param numByRow the number of submatrices along the row side you assign, the real num may be not.
   * @param numByCol the number of submatrices along the column side you assign, the real num may be not.
   * @param distribution the distribution of the elements in the matrix, default is U[0.0, 1.0]
   * @return BlockMatrix
   */
  def randomBlockMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Long,
      numByRow: Int,
      numByCol: Int,
      distribution: RandomDataGenerator[Double] = new UniformGenerator(0.0, 1.0)): Blocked = {

    val mRows = nRows.toInt
    val mColumns = nColumns
    val mBlockRowSize = math.ceil(mRows.toDouble / numByRow.toDouble).toInt
    val mBlockColSize = math.ceil(mColumns.toDouble / numByCol.toDouble).toInt
    val blksByRow = math.ceil(mRows.toDouble / mBlockRowSize).toInt
    val blksByCol = math.ceil(mColumns.toDouble / mBlockColSize).toInt

    val blocks = RandomRDDs.randomBlockRDD(sc, distribution, nRows, nColumns, blksByRow, blksByCol)
    new Blocked(blocks, nRows, nColumns, blksByRow, blksByCol)
  }

  /**
   * Function to generate a distributed BlockMatrix, in which every element is in the uniform distribution
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @param numPartitions the partitions you want to assign
   * @param distribution the distribution of the elements in the matrix, default is U[0.0, 1.0]
   * @return DenseVecMatrix
   */
  def randomDenVecMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int,
      numPartitions: Int = 0,
      distribution: RandomDataGenerator[Double] = new UniformGenerator(0.0, 1.0)): DenseVectored = {
    
    val rows = RandomRDDs.randomDenVecRDD(sc, distribution, nRows, nColumns, numPartitionsOrDefault(sc, numPartitions))
    new DenseVectored(rows, nRows, nColumns)
  }

  def randomSpaVecMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int,
      density: Double,
      numPartitions: Int = 0,
      distribution: RandomDataGenerator[Double] = new UniformGenerator(0.0, 1.0)): SparseVectored = {

    val rows = RandomRDDs.randomSpaVecRDD(sc, distribution, nRows, nColumns,
      numPartitionsOrDefault(sc, numPartitions), density)
    new SparseVectored(rows, nRows, nColumns)
  }

  def randomDistVector(sc: SparkContext,
      length: Long,
      numSplits: Int,
      distribution: RandomDataGenerator[Double] = new UniformGenerator(0.0, 1.0)): DistributedVector = {
    val parts = RandomRDDs.randomDistVectorRDD(sc, distribution, length, numSplits)
    new DistributedVector(parts, length, numSplits)
  }

  /**
   *  Function to generate a distributed BlockMatrix, in which every element is zero
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @return DenseVecMatrix
   */
  def zerosDenVecMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int): DenseVectored = {

    val rows = RandomRDDs.zerosDenVecRDD(sc, nRows, nColumns)
    new DenseVectored(rows, nRows, nColumns)
  }

  /**
   *  Function to generate a distributed BlockMatrix, in which every element is one
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @return DenseVecMatrix
   */
  def onesDenVecMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int): DenseVectored = {

    val rows = RandomRDDs.onesDenVecRDD(sc, nRows, nColumns)
    new DenseVectored(rows, nRows, nColumns)
  }


  def onesDistVector(sc: SparkContext,
      length: Long,
      numSplits: Int): DistributedVector = {
    val ones = new OnesGenerator()
    val parts = RandomRDDs.randomDistVectorRDD(sc, ones, length, numSplits)
    new DistributedVector(parts)
  }



  /**
   * Function to design the method how to split input two matrices, this method refer to CARMA
   *  [[http://www.eecs.berkeley.edu/~odedsc/papers/bfsdfs-mm-ipdps13.pdf]], we recommend you to
   *  design your own split method
   *
   * @param m rows num of Matrix A
   * @param k columns num of Matrix A, also rows num of Matrix B
   * @param n columns num of Matrix B
   * @param cores all the cores cross the cluster
   * @return rows of Matrix A to be split nums, columns of Matrix A to be split nums,
   *         columns of Matrix B to be split nums
   */
  private[mctrix] def splitMethod(m: Long, k: Long, n: Long, cores: Int): (Int, Int, Int) = {
    var mSplitNum = 1
    var kSplitNum = 1
    var nSplitNum = 1
    var _m = m
    var _k = k
    var _n = n
    var _cores = cores
    val factor = 2
    while (_cores > 1 && _m > 1 && _k > 1 && _n > 1) {
      if (dimToSplit(_m, _k, _n) == 1) {
        nSplitNum *= factor
        _n /= factor
        _cores /= factor
      } else if (dimToSplit(_m, _k, _n) == 2) {
        mSplitNum *= factor
        _m /= factor
        _cores /= factor
      } else {
        kSplitNum *= factor
        _k /= factor
        _cores /= factor
      }
    }
    (mSplitNum, kSplitNum, nSplitNum)
  }

  /**
   * generate the split method
   * @param oldRange
   * @param newSubBlk the sub-block length of the new matrix
   */
  private[mctrix] def splitMethod(oldRange: Array[(Int, Int)], newSubBlk: Int):
        Array[ArrayBuffer[(Int, (Int, Int), (Int, Int))]] = {
    val oldBlks = oldRange.length
    val splitStatus = Array.ofDim[ArrayBuffer[(Int, (Int, Int),(Int, Int))]](oldBlks)
    for (i <- 0 until oldBlks){
      val (start, end) = oldRange(i)
      val startId = start / newSubBlk
      val endId = end / newSubBlk
      val num = endId - startId + 1
//      val tmpBlk = math.ceil((end - start + 1).toDouble / num.toDouble).toInt
      val arrayBuffer = new ArrayBuffer[(Int, (Int, Int), (Int, Int))]()
      var tmp = 0
      for (j <- 0 until num){
        val tmpEnd = min((j + startId + 1) * newSubBlk - 1 - start , end - start)
        arrayBuffer.+=((j + startId, (tmp , tmpEnd), ((tmp + start) % newSubBlk, (tmpEnd + start) % newSubBlk)))
        tmp = tmpEnd + 1
      }
      splitStatus(i) = arrayBuffer
    }
    splitStatus
  }

  private def dimToSplit(m: Long, k: Long, n: Long): Int={
    var result = 0
    if (n >= k && n >= m) {
      result = 1
    }else if (m >= k && m >= n) {
      result = 2
    }
    else result = 3
    result
  }

  /**
   * Function to load Coordinate matrix from file
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
  */
  def loadCoordinateMatrix(sc: SparkContext, path: String, minPartitions: Int = 4): Coordinated = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://")
      && !path.startsWith("/") && !path.startsWith("~/")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      throw new IllegalArgumentException("the path is not in local file System, HDFS or Tachyon")
    }
    val data = sc.textFile(path, minPartitions)
    val entries = data.map(_.split(",\\s?|\\s+") match {
      case Array(rowId, colId, value) =>
        ((rowId.toLong, colId.toLong), value.toFloat)
      // like MovieLens data, except the (user, product, rating) there still exist time stamp data
      case Array(rowId, colId, value, timeStamp) =>
        ((rowId.toLong, colId.toLong), value.toFloat)
    })
    new Coordinated(entries)
  }

  /**
   * Function to load SVM like file, actually the first item of each line is not the label but the line index
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param vectorLen the length of each vector
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return
   */
  def loadSVMDenVecMatrix(sc: SparkContext, path: String, vectorLen: Int, minPartitions: Int = 4): DenseVectored = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://")
      && !path.startsWith("/") && !path.startsWith("~/")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      throw new IllegalArgumentException("the path is not in local file System, HDFS or Tachyon")
    }
    val data = sc.textFile(path, minPartitions)
    val rows = data.map { line =>
      val items = line.split(" ")
      val index = items.head.toLong
      val indicesAndValues = items.tail.map{ item =>
        val indexAndValue = item.split(":")
        val ind = indexAndValue(0).toInt - 1
        val value = indexAndValue(1).toDouble
        (ind, value)
      }
      val array = Array.ofDim[Double](vectorLen)
      for ((i, v) <- indicesAndValues){
        array.update(i, v)
      }
      (index, BDV(array))
    }
    new DenseVectored(rows, 0L, vectorLen)
  }

  /**
   * Function to load matrix from file
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a distributed matrix in DenseVecMatrix type                    
   */
  def loadMatrixFile(sc: SparkContext, path: String, minPartitions: Int = 4): DenseVectored = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://")
      && !path.startsWith("/") && !path.startsWith("~/")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      throw new IllegalArgumentException("the path is not in local file System, HDFS or Tachyon")
    }
    val file = sc.textFile(path, minPartitions)
    val rows = file.map(t =>{
      val e = t.split(":")
      val rowIndex = e(0).toLong
      val array = e(1).split(",\\s?|\\s+").map(_.toDouble)
      val vec = BDV(array)
      (rowIndex,vec)
    })
    new DenseVectored(rows)
  }

//  /**
//   * Load DenseVecMatrix from sequence file, the original sequenceFile is key-value pair stored,
//   * key is index in `Long` type, value is `DenseVector`
//   * s
//   *@param sc the running SparkContext
//   * @param path the path where store the matrix
//   * @param minPartitions the min num of partitions of the matrix to load in Spark
//   */
//  def loadMatrixSeqFile(sc: SparkContext, path: String, minPartitions: Int = 0): DenseVecMatrix = {
//    val result = sc.sequenceFile[Long, DenseVector](path, numPartitionsOrDefault(sc, minPartitions))
//    new DenseVecMatrix(result)
//  }


  /**
   * Function to load block matrix from file
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a distributed matrix in BlockMatrix type 
   */
  def loadBlockMatrixFile(sc: SparkContext, path: String, minPartitions: Int = 4): Blocked = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val file = sc.textFile(path, minPartitions)
    println("RDD length: " + file.count())
    val blocks = file.map(t =>{
        val e = t.split(":")
        val info = e(0).split("-")
        val array = e(1).split(",\\s?|\\s+").map(_.toDouble)
        (new BlockID(info(0).toInt, info(1).toInt), new BDM[Double](info(2).toInt, info(3).toInt, array))
      })
    new Blocked(blocks)
  }

  /**
   * Function to load matrix from a dictionary which contains many files to generate a Matrix
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a distributed matrix in DenseVecMatrix type                    
   */
  def loadMatrixFiles(sc: SparkContext, path: String, minPartitions: Int = 4): DenseVectored = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val files = sc.wholeTextFiles(path, minPartitions)
    val rows = files.filter(t => !t._2.isEmpty).flatMap(t =>{
      val lines = t._2.split("\n")
      lines.map( l =>{
        val content = l.split(":")
        ( content(0).toLong, BDV(content(1).split(",\\s?|\\s+").map(_.toDouble) ))
      })
    })
    new DenseVectored(rows)
  }

  /**
   * Function to load block matrix from a dictionary which contains many files to generate a Matrix
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a disrtibuted matrix in BlockMatrix type                    
   */
  def loadBlockMatrixFiles(sc: SparkContext, path: String, minPartitions: Int = 4): Blocked = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      throw new IllegalArgumentException()
    }
    val file = sc.wholeTextFiles(path, minPartitions)
    val blocks = file.flatMap(t =>{
      val blks = t._2.split("\n")
      blks.map( b =>{
        val e = b.split(":")
        val info = e(0).split("-")
        val array = e(1).split(",\\s?|\\s+").map(_.toDouble)
        (new BlockID(info(0).toInt, info(1).toInt), new BDM[Double](info(2).toInt, info(3).toInt, array))
      })
    })
    new Blocked(blocks)
  }

  /**
   * Function to generate a DenseVecMatrix from a Array[Array[Double]
   *
   * @param sc the running SparkContext
   * @param array the two dimension Double array
   * @param partitions the default num of partitions when you create an RDD, you can set it by yourself
   * @return a distributed matrix in DenseVecMatrix type                  
   */
  def arrayToMatrix(sc:SparkContext , array: Array[Array[Double]] , partitions: Int = 2): DenseVectored ={
    new DenseVectored( sc.parallelize(array.zipWithIndex
      .map{case(t,i)  => (i.toLong, BDV(t)) },partitions) )
  }

  /**
   * Function to transform a DenseVecMatrix to a Array[Array[Double]], but now we use collect method to
   * get all the content from distributed matrices which means it is expensive, so currently only
   * not so large matrix can transform to Array[Array[Double]]
   *
   * @param mat the DenseVecMatrix to be transformed
   * @return a local array of two dimensions           
   */

  def matrixToArray(mat: Distributed ): Array[Array[Double]] ={
    mat match {
      case m: DenseVectored=> {
        val array = Array.ofDim[Double](m.numRows().toInt, m.numCols().toInt)
        m.rows.collect().foreach(t => t._2.toArray.copyToArray(array(t._1.toInt)) )
        array
      }
      case m: Blocked => {
        val blkRowSize = math.ceil(m.numRows().toDouble / m.numBlksByRow().toDouble).toInt
        val blkColSize = math.ceil(m.numCols().toDouble / m.numBlksByCol().toDouble).toInt
        val array = Array.ofDim[Double](m.numRows().toInt, m.numCols().toInt)
        m.blocks.collect().foreach(t => {
          val rowBase = t._1.row * blkRowSize
          val colBase = t._1.column * blkColSize
          val iterator = t._2.iterator
          while (iterator.hasNext) {
            val ((r, c), v) = iterator.next()
            array(r + rowBase)(c + colBase) = v
          }
        })
        array
      }
    }

  }

  /**
   * This function is used to improve the matrix multiply performance, just broadcast a local
   * matrix, avoid the overhead when collecting the distributed matrix
   *
   * @param localMat a local breeze.linalg.DenseMatrix
   * @param matrix the distributed matrix
   * @param cores the num of cores across the cluster
   * @return
   */
  def leftBroadMultiply(localMat: BDM[Double], matrix: Distributed, cores: Int): Blocked = {
    require(localMat.cols == matrix.numRows(), s"matrices dimension mismatched: ${localMat.cols} vs ${matrix.numRows()}")
    matrix match {
      case that: Blocked => {
        val brodMat = that.blocks.context.broadcast(localMat)
        val result = that.blocks.mapPartitions(iter => {
          iter.map( t => {
            (t._1, (brodMat.value * t._2).asInstanceOf[BDM[Double]])
          })
        })
        new Blocked(result, localMat.rows, that.numCols(), that.numBlksByRow(), that.numBlksByCol())
      }
      case that: DenseVectored => {
        val brodMat = that.rows.context.broadcast(localMat)
        val blkMat = that.toBlockMatrix(1, math.min(8 * cores, that.numCols().toInt / 2))
        val result = blkMat.blocks.mapPartitions(iter => {
          iter.map( t => {
            (t._1, (brodMat.value * t._2).asInstanceOf[BDM[Double]])
          })
        })
        new Blocked(result, localMat.rows, that.numCols(), 1, math.min(8 * cores, that.numCols().toInt / 2))
      }
    }
  }

  /**
   * Like function rep in R, repeat the matrix by row to create a new matrix
   * @param matrix the matrix to be repeated
   * @param times the times to repeat the matrix, 1 means do nothing, 2 means repeat once
   */
  def repeatByRow(matrix: Distributed, times: Int): Distributed = {
    require(times > 0, s"repeat times: $times illegal")
    if (times == 1){
      matrix
    }else {
      matrix match {
        case vecMatrix: DenseVectored => {
          val result = vecMatrix.rows.map( t =>
            (t._1, BDV(List.fill(times)(t._2.toArray).flatten.toArray)) )
          new DenseVectored(result, vecMatrix.numRows(), vecMatrix.numCols() * times)
        }
        case blockMatrix: Blocked => {
          val result = blockMatrix.blocks.flatMap( t => {
            for ( i <- 0 until times)
            yield (new BlockID(t._1.row, t._1.column + i * blockMatrix.numBlksByCol()), t._2)
          })
          new Blocked(result, blockMatrix.numRows(),
            blockMatrix.numCols(), blockMatrix.numBlksByRow(), blockMatrix.numBlksByCol() * times)
        }
      }
    }
  }

  /**
   * Like function rep in R, repeat the matrix by column to create a new matrix
   * @param matrix the matrix to be repeated
   * @param times the times to repeat the matrix, 1 means do nothing, 2 means repeat once
   */
  def repeatByColumn(matrix: Distributed, times: Int): Distributed = {
    require(times > 0, s"repeat times: $times illegal")
    if (times == 1){
      matrix
    }else {
      matrix match  {
        case vecMatrix: DenseVectored => {
          val result = vecMatrix.rows.flatMap(t => {
            for ( i <- 0 until times)
            yield (t._1 + i * vecMatrix.numRows(), t._2)
          })
          new DenseVectored(result, vecMatrix.numRows() * times, vecMatrix.numCols())
        }
        case  blockMatrix: Blocked => {
          val result = blockMatrix.blocks.flatMap(t =>{
            for ( i <- 0 until times)
            yield (new BlockID(t._1.row + i * blockMatrix.numBlksByRow(), t._1.column), t._2)
          })
          new Blocked(result, blockMatrix.numRows() * times, blockMatrix.numCols()
            , blockMatrix.numBlksByRow() * times, blockMatrix.numBlksByCol())
        }
      }
    }
  }

  /**
   * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
   */
  private def numPartitionsOrDefault(sc: SparkContext, numPartitions: Int): Int = {
    if (numPartitions > 0) numPartitions
    else if (!sc.getConf.getOption("spark.default.parallelism").isEmpty) {
      sc.getConf.get("spark.default.parallelism").toInt
    }else 
      sc.defaultMinPartitions
  }


}