package com.mapreduce
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import java.io.IOException
import java.util.StringTokenizer

class MapData extends Mapper[LongWritable, Text, Text, IntWritable] {
  
  @throws(classOf[IOException])
  def map(key:LongWritable ,value:Text,context:Context): Unit = {
    var line = value.toString()
    var tokenizer = new StringTokenizer(line)
    
    while(tokenizer.hasMoreTokens()){
      value.set(tokenizer.nextToken())
      context.write(value, new IntWritable(1))
    }
  }
}