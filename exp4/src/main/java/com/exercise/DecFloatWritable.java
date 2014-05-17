package com.exercise;

import org.apache.hadoop.io.FloatWritable;

public class DecFloatWritable extends FloatWritable{
	public DecFloatWritable(float value){
		super(value);
	}
	public DecFloatWritable(){
	}
	@Override
	public int compareTo(Object o) {
		return -super.compareTo(o);
	}
}
