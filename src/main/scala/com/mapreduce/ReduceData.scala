package com.mapreduce

import java.io.IOException

import org.apache.hadoop.io.IntWritable

import org.apache.hadoop.io.Text

import org.apache.hadoop.mapreduce.Reducer


import scala.collection.JavaConversions._

class ReduceData extends Reducer[Text, IntWritable, Text, IntWritable] {

  def reduce(key: Text,
             values: java.lang.Iterable[IntWritable],
             context: Context): Unit = {
    var sum: Int = 0
    for (x <- values) {
      sum += x.get
    }
    context.write(key, new IntWritable(sum))
  }

}
