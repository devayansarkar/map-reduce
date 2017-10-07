package com.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.fs.Path

object Counter{
    def main(args:Array[String]):Unit = {
       val conf =  new Configuration()
    val job = Job.getInstance(conf,"WordCount")
    
    job.setJarByClass(classOf[MapData])
    
    job.setMapperClass(classOf[MapData])
    job.setCombinerClass(classOf[ReduceData])
    job.setReducerClass(classOf[ReduceData])
   
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    
    
    var outputPath = new Path(args(1))
    var inputPath = new Path(args(0))
    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    
    outputPath.getFileSystem(conf).delete(outputPath,true)
    System.exit(if(job.waitForCompletion(true))0 else 1)
 
    }
  }