/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author eduardo
 */
public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private Log log;
    private IntWritable valueOut;
    private Text keyOut;
    
    @Override
	protected void setup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
    	log = LogFactory.getLog(Map1.class);
    	valueOut = new IntWritable(1);
    	keyOut = new Text();
    	
    	log.info("AprioriCpa Map para encontrar L1");
    }

	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException{
        
        StringTokenizer token = new StringTokenizer(value.toString());
        while(token.hasMoreTokens()){
        	keyOut.set(token.nextToken());
            try {
                context.write(keyOut, valueOut);
            } catch (InterruptedException ex) {
                Logger.getLogger(Map1.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		log = null;
		valueOut = null;
		keyOut = null;
	}
}
