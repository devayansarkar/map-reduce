package com.mapreduce

object Counter{
    def main(args:Array[String]):Unit = {
       val wordCount = new WordCount
       wordCount.countWords(args)
    }
  }