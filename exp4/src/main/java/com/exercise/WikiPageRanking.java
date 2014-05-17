package com.exercise;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WikiPageRanking {
	 
    private static NumberFormat nf = new DecimalFormat("00");
 
    public static void main(String[] args) throws Exception {
        WikiPageRanking pageRanking = new WikiPageRanking();
 
        //Job 1: Parse XML
        {
	        String[] funcArgs = {args[0], "wiki/ranking/iter00"};
	        pageRanking.runXmlParsing(funcArgs);
        }
 
        int runs = 0;
        for (; runs < 5; runs++) {
            //Job 2: Calculate new rank
        	String[] funcArgs = {"wiki/ranking/iter"+nf.format(runs), "wiki/ranking/iter"+nf.format(runs + 1)};
            pageRanking.runRankCalculation(funcArgs);
        }
 
        //Job 3: Order by rank
        {
            String[] funcArgs = {"wiki/ranking/iter"+nf.format(runs), "wiki/result"};
            pageRanking.runRankViewer(funcArgs);
        }
        
        System.exit(0);
    }
 
    public void runXmlParsing(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
		
		Job job = new Job(conf, "PageLinkProcess");
		job.setJarByClass(WikiPageRanking.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Mapper & Reducer
		job.setMapperClass(WikiPageLinksMapper.class);
		job.setReducerClass(WikiPageLinksReducer.class);
		
		// Map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reduce
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
    }
 
    private void runRankCalculation(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	Configuration conf = new Configuration();
		Job job = new Job(conf, "PageRankIter");
		job.setJarByClass(WikiPageRanking.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Mapper & Reducer
		job.setMapperClass(PageRankIterMapper.class);
		job.setReducerClass(PageRankIterReducer.class);
		
		// Map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reduce
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
    }
 
    private void runRankViewer(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "PageRankViewer");
    	job.setJarByClass(WikiPageRanking.class);
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	//Mapper
    	job.setMapperClass(PageRankViewerMapper.class);
    	
    	//Map
    	job.setMapOutputKeyClass(DecFloatWritable.class);
    	job.setMapOutputValueClass(Text.class);
    	
    	//Reduce
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	job.waitForCompletion(true);	
    }
}
