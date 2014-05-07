package com.exercise;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
	private static final float damping = 0.85F;
	 
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] split;
        float sumShareOtherPageRanks = 0;
        int N = 1;
        float originalRank = 0;
        String links = "";
        String pageWithRank;
 
        // For each otherPage:
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        Iterator<Text> it = values.iterator();
        while(it.hasNext()){
            pageWithRank = it.next().toString();
 
            if(pageWithRank.startsWith("!")) {
            	originalRank = Float.valueOf(pageWithRank.substring(1));
                continue;
            }
 
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }
            
            if (pageWithRank.startsWith("c")) {
//            	context.write(key, new Text(pageWithRank.substring(1)));
//            	return;
            	N = Integer.valueOf(pageWithRank.substring(1));
            	continue;
            }
 
            split = pageWithRank.split(conf.splitter);
 
            float pageRank = Float.valueOf(split[0]);
            int countOutLinks = Integer.valueOf(split[1]);
 
            sumShareOtherPageRanks += (pageRank/countOutLinks);
        }
 
        float newRank = damping * sumShareOtherPageRanks + (1-damping) * originalRank / N;
 
        context.write(key, new Text(newRank + links));
    }
}