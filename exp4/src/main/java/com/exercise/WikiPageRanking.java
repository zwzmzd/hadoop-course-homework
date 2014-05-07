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
        for (; runs < 1; runs++) {
            //Job 2: Calculate new rank
        	String[] funcArgs = {"wiki/ranking/iter"+nf.format(runs), "wiki/ranking/iter"+nf.format(runs + 1)};
            pageRanking.runRankCalculation(funcArgs);
        }
 
        //Job 3: Order by rank
        //pageRanking.runRankOrdering("wiki/ranking/iter"+nf.format(runs), "wiki/result");
        System.exit(0);
 
    }
 
    public void runXmlParsing(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        JobConf conf = new JobConf(WikiPageRanking.class);
// 
//        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
//        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
// 
//        // Input / Mapper
//        FileInputFormat.setInputPaths(conf, new Path(inputPath));
//        conf.setInputFormat(XmlInputFormat.class);
//        conf.setMapperClass(WikiPageLinksMapper.class);
// 
//        // Output / Reducer
//        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//        conf.setOutputFormat(TextOutputFormat.class);
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(Text.class);
//        conf.setReducerClass(WikiLinksReducer.class);
// 
//        JobClient.runJob(conf);
        
        Configuration conf = new Configuration();
		
		Job job = new Job(conf, "PageLinkProcess");
		job.setJarByClass(WikiPageRanking.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// ����Mapper��Reducer
		job.setMapperClass(WikiPageLinksMapper.class);
		job.setReducerClass(WikiLinksReducer.class);
		
		// Map�׶β���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reduce�׶β���
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
		
		// ����Mapper��Reducer
		job.setMapperClass(PageRankIterMapper.class);
		job.setReducerClass(PageRankIterReducer.class);
		
		// Map�׶β���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reduce�׶β���
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
    }
 
/*
    private void runRankOrdering(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(WikiPageRanking.class);
 
        conf.setOutputKeyClass(FloatWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
 
        conf.setMapperClass(RankingMapper.class);
 
        JobClient.runJob(conf);
    }
*/
}
