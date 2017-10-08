package com.mapreduce

import java.io.IOException

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.mapreduce.Job

import org.apache.hadoop.io.Text

import org.apache.hadoop.io.IntWritable

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.hadoop.fs.Path


import scala.collection.JavaConversions._

object Counter {

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    val job: Job = new Job(conf, "WordCount")
    job.setJarByClass(classOf[MapData])
    job.setMapperClass(classOf[MapData])
    job.setReducerClass(classOf[ReduceData])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[_,_]])
    val intputPath: Path = new Path(args(0))
    val outputPath: Path = new Path(args(1))
    FileInputFormat.addInputPath(job, intputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    outputPath.getFileSystem(conf).delete(outputPath)
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }

}
