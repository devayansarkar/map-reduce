package com.mapreduce
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class MapData extends Mapper[LongWritable, Text, Text, IntWritable] {
  
}