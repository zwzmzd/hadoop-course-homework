package com.exercise;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	String phones1 = 
                "Justin 的手机号码：0939-100391\n" + 
                "momor 的手机号码：0939-666888\n"; 
          
          Pattern pattern = Pattern.compile("&lttitle&gt(.*?)&lt\\/title&gt"); 
          Matcher matcher = pattern.matcher(phones1); 

          while(matcher.find()) { 
              System.out.println(matcher.group()); 
          } 
          
          String phones2 = 
        		  "&lttitle&gtAfricanAmericanPeople&lt/title&gt    &ltid&gt241&lt/id&gt    &ltrevision&gt      &ltid&gt74467491&lt/id&gt      &lttimestamp&gt2006-09-08T04:22:44Z&lt/timestamp&gt      &ltcontributor&gt        &ltusername&gtRory096&lt/username&gt        &ltid&gt750223&lt/id&gt      &lt/contributor&gt      &ltcomment&gtcat rd&lt/comment&gt      &lttext xml:space=\"preserve\"&gt#REDIRECT [[African American]] {{R from CamelCase}}&lt/text&gt    &lt/revision&gt"; 
          
          matcher = pattern.matcher(phones2); 

          while(matcher.find()) { 
        	  System.out.println(matcher.groupCount());
              System.out.println(matcher.group(1)); 
          } 
    }
}
