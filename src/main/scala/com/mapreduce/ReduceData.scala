package com.mapreduce

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import java.io.IOException
import java.lang.Iterable
import scala.collection.JavaConversions._


class ReduceData extends Reducer[Text,IntWritable,Text,IntWritable]{
  
  @throws(classOf[IOException])
  override
  def reduce(key:Text, values:Iterable[IntWritable],context:Reducer[Text,IntWritable,Text,IntWritable]#Context)= {
    val sum = values.foldLeft(0){(total, value ) => total + value.get}
    context.write(key,new IntWritable(sum))
  }
  
}