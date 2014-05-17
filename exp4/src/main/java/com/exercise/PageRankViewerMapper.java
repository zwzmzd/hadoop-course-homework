package com.exercise;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class PageRankViewerMapper extends Mapper<Object, Text, DecFloatWritable, Text>{
    
	public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
		int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex+1);
 
        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageRank = Text.decode(value.getBytes(), pageTabIndex + 1, rankTabIndex - pageTabIndex - 1);

		float PR = Float.parseFloat(pageRank);
		context.write(new DecFloatWritable(PR), new Text(page));
	}
}
