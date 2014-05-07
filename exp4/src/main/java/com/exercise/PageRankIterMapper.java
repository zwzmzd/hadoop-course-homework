package com.exercise;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankIterMapper extends Mapper<Object, Text, Text, Text> {
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException
	{
		int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex+1);
 
        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageRank = Text.decode(value.getBytes(), pageTabIndex + 1, rankTabIndex - pageTabIndex - 1);
 
        // Mark page as an Existing page (ignore red wiki-links)
        context.write(new Text(page), new Text("!" + pageRank));
 
        // Skip pages with no links.
        if(rankTabIndex == -1) return;
 
        String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
        String[] allOtherPages = links.split(",");
        int totalLinks = allOtherPages.length;
        
        context.write(new Text(page), new Text("c" + totalLinks));
 
        for (String otherPage : allOtherPages){
            Text pageRankTotalLinks = new Text(pageRank + conf.splitter + totalLinks);
            context.write(new Text(otherPage), pageRankTotalLinks);
        }
 
        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|"+links));
	}
}