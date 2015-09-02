package edu.hit.dblab.mctrix.matrix

import breeze.linalg.{DenseVector=>BDV}
import org.apache.spark.rdd.RDD

/**
 *
 */
class UpperTriangular(size:Long,  vectors:RDD[(Long,BDV[Double])], rowMajor:Boolean) {
  def this(vectors:RDD[(Long,BDV[Double])], rowMajor:Boolean)=this(0L, vectors, rowMajor)
  def this(size:Long,vectors:RDD[(Long,BDV[Double])])=this(size,vectors,true)
  def this(vectors:RDD[(Long,BDV[Double])])=this(0L, vectors, true)

  def getSize():Long = {
    if(size==0){
      vectors.map{v=>v._1}.reduce((a,b)=>{if(a>b)a else b})
    }else {
      size
    }
  }
  def validate(): Boolean ={
     vectors.map{
      case (index, data)=>{index==data.length-1}
      }.reduce(_&_)
    }

  def solve(rightEnd:BDV[Double]):BDV[Double]={
    var result:RDD[(Long,BDV[Double],Double)]=vectors.map{case(index,data)=>(index,data,rightEnd(index.toInt))}
    for(i <- 0L to  size-1) {
      var ite=size-1-i
      result={
        val ithBroadcast =result
          .filter{case(index,data,rightItem)=>{index==ite}}
          .map{case(index,data,rightItem)=>rightItem/data(index.toInt)}
          .collect()(0)
        result.map{case(index,data,rightItem)=>
          if(index==ite){
            (index,BDV[Double](),ithBroadcast)
          }else if(index<ite){
            (index,data,rightItem-data(ite.toInt)*ithBroadcast)
          }else{
            (index,data,rightItem)
          }
        }
      }
    }
    BDV(result.sortBy{case(index,data,rightItem)=>index}.map{case(index,data,rightItem)=>rightItem}.collect())


  }
}
class LowerTriangular(size:Long,  vectors:RDD[(Long,BDV[Double])], rowMajor:Boolean){
  def this(vectors:RDD[(Long,BDV[Double])], rowMajor:Boolean)=this(0L, vectors, rowMajor)
  def this(size:Long,vectors:RDD[(Long,BDV[Double])])=this(size,vectors,true)
  def this(vectors:RDD[(Long,BDV[Double])])=this(0L, vectors, true)

  def getSize():Long = {
    if(size==0){
      vectors.map{v=>v._1}.reduce((a,b)=>{if(a>b)a else b})
    }else {
      size
    }
  }
  def validate(): Boolean ={
    vectors.map{
      case (index, data)=>{index+data.length==size}
    }.reduce(_&_)
  }

  def solve(rightEnd:BDV[Double]):BDV[Double]={
    var result:RDD[(Long,BDV[Double],Double)]=vectors.map{case(index,data)=>(index,data,rightEnd(index.toInt))}
    for(i <- 0L to  size-1) {
      result={
        val ithBroadcast =result
          .filter{case(index,data,rightItem)=>{index==i}}
          .map{case(index,data,rightItem)=>rightItem/data(index.toInt)}
        .collect()(0)
        result.map{case(index,data,rightItem)=>
          if(index==i){
            (index,BDV[Double](),ithBroadcast)
          }else if(index>i){
            (index,data,rightItem-data(i.toInt)*ithBroadcast)
          }else{
            (index,data,rightItem)
          }
        }
      }
    }
    BDV(result.sortBy{case(index,data,rightItem)=>index}.map{case(index,data,rightItem)=>rightItem}.collect())




  }
}
