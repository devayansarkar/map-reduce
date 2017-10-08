package com.mapreduce
import java.io.IOException

import java.util.StringTokenizer

import org.apache.hadoop.io.IntWritable

import org.apache.hadoop.io.LongWritable

import org.apache.hadoop.io.Text

import org.apache.hadoop.mapreduce.Mapper

import scala.collection.JavaConversions._

class MapData extends Mapper[LongWritable, Text, Text, IntWritable] {

  def map(key: LongWritable, value: Text, context: Context): Unit = {
    val line: String = value.toString
    val tokenizer: StringTokenizer = new StringTokenizer(line)
    while (tokenizer.hasMoreTokens()) {
      value.set(tokenizer.nextToken())
      context.write(value, new IntWritable(1))
    }
  }

}