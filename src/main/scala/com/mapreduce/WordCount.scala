package com.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class WordCount {
  
  def countWords(args:Array[String]): Unit = {
    val conf =  new Configuration
    val job = Job.getInstance(conf,"WordCount")
    
    job.setJarByClass(classOf[WordCount])
    job.setMapperClass(classOf[MapData])
    job.setReducerClass(classOf[ReduceData])
    
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]]) 
    
    var outputPath = new Path(args(1))
    
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, outputPath)
    
    outputPath.getFileSystem(conf).delete(outputPath,true)
    System.exit(if(job.waitForCompletion(true))0 else 1)
    }
  
}