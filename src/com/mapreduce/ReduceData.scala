package com.mapreduce

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class ReduceData extends Reducer[Text,IntWritable,Text,IntWritable]{
  
  def reduce(key:Text, values:Iterable[IntWritable],context:Context):Unit={
    var sum = 0 
    for(value <- values){
      sum+=value.get()
    }
    context.write(key,new IntWritable(sum))
  }
  
}