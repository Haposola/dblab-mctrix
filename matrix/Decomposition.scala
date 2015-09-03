package edu.hit.dblab.mctrix.matrix

import breeze.linalg.{DenseVector=>BDV}
/**
 * Created by haposola on 15-9-3.
 */
class LU (inputMatrix: DenseVectored){
  var L : LowerTriangular=_
  var U:UpperTriangular=_
  private var col=inputMatrix.numCols()
  private val row=inputMatrix.numRows()
  private val vectors=inputMatrix.rows
  def getL=L
  def getU=U

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
