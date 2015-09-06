package edu.hit.dblab.mctrix.matrix

import breeze.linalg.{DenseVector => BDV, norm}
import edu.hit.dblab.mctrix.vector.Vectors

/**
 * Created by haposola on 15-9-3.
 */
class LU (inputMatrix: DenseVectored){
  private var col=inputMatrix.numCols()
  private val row=inputMatrix.numRows()
  private val vectors=inputMatrix.rows


  def compute:(LowerTriangular,UpperTriangular)={
    var interResult=vectors.map{case(index,data)=>((index,data),(index,new BDV[Double](new Array[Double](0))))}
    for(i<- 0L to row-1){
      var ithBroadcast =interResult.filter{case((indexU,dataU),(indexL,dataL))=>indexU==i}.collect()(0)._1._2
      interResult={
        interResult.map{case((indexU,dataU),(indexL,dataL))=>
          if(indexU==i){
            ((indexU,dataU),(indexL,new BDV((dataL.data.toBuffer+=1.0).toArray)))
          }else if(indexU>i){
            val ratio=dataU(0)/ithBroadcast(0)
            val newU=(dataU-ratio*ithBroadcast).data.toBuffer
            newU.remove(0)
            ((indexU,new BDV(newU.toArray)), (indexL,new BDV((dataL.data.toBuffer+=ratio).toArray)))
          }else{
            ((indexU,dataU),(indexL,dataL))
          }
        }
      }
    }
    (new LowerTriangular(interResult.map{case((indexU,dataU),(indexL,dataL))=>(indexL,dataL)})
      ,
      new UpperTriangular(interResult.map{case((indexU,dataU),(indexL,dataL))=>(indexU,dataU)})
      )
  }
}

/*
  *QR Decomposition receives a matrix which is column majored.
  * TODO: A R-Major-TO-C-Major function is required in DenseVectored class
 */
class QR(inputMatrix:DenseVectored){
  private val col=inputMatrix.numCols()
  private val row=inputMatrix.numRows()
  private val vectors=inputMatrix.rows

  def compute():(DenseVectored,UpperTriangular)={
    var interResult = vectors.map{case(indexQ,dataQ)=>
      ((indexQ,dataQ),(indexQ,Vectors.denseOneInK((indexQ+1).toInt,indexQ.toInt,1.0)))
    }
    for(i<- 0L to col-1){
      interResult= {
        val ithBroadcast = interResult
          .filter{case((indexQ,dataQ),(indexR,dataR))=>indexQ==i}
          .map{case((indexQ,dataQ),(indexR,dataR))=>dataQ}
          .collect()(0)
        var square=0.0
        for(i<- 0 to ithBroadcast.length-1)
          square+=Math.pow(ithBroadcast(i.toInt),2)
        interResult.map {case((indexQ,dataQ),(indexR,dataR))=>
          if(indexQ==i){
            dataR.update(indexQ.toInt,Math.sqrt(square))
            ((indexQ,dataQ/Math.sqrt(square)),(indexR,dataR))
          }else if(indexQ>i){
            val dot:Double= dataQ.dot( ithBroadcast)
            dataR.update(i.toInt,dot/Math.sqrt(square))
            ((indexQ,dataQ-dot/square*ithBroadcast),(indexR,dataR))
          }else{
            ((indexQ,dataQ),(indexR,dataR))
          }
        }

      }
    }
    (
      new DenseVectored(interResult.map{case((indexQ,dataQ),(indexR,dataR))=>(indexQ,dataQ)})
      ,
      new UpperTriangular(interResult.map{case((indexQ,dataQ),(indexR,dataR))=>(indexR,dataR)})
      )
  }



}