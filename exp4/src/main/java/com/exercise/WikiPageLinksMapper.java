package com.exercise;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiPageLinksMapper extends Mapper<LongWritable, Text, Text, Text> {
	
    private static final Pattern titlePattern = Pattern.compile("&lttitle&gt(.*?)&lt\\/title&gt");
	private static final Pattern wikiLinksPattern = Pattern.compile("#REDIRECT \\[\\[([^\\[]*)\\]\\]");

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 匹配正则
		String lineText = value.toString();
		Matcher titleMatcher = titlePattern.matcher(lineText);
		Matcher linksMatcher = wikiLinksPattern.matcher(lineText);

		// 一个title，对应着 0-多个link
		if (titleMatcher.find() && titleMatcher.groupCount() == 1) {
			String title = titleMatcher.group(1).replaceAll(" ", "");
			title = title.toLowerCase();
			while (linksMatcher.find()) {
				if (linksMatcher.groupCount() == 1) {
					String link = linksMatcher.group(1).replaceAll(" ", "");
					link = link.toLowerCase();
					context.write(new Text(title), new Text(link));
				}
			}
		}
	}
}
