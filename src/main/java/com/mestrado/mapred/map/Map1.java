/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
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
		String[] tr;
		boolean endBlock = false;
		int pos;
		int start = 0;
		int len;

		while ((pos = value.find("\n", start)) != -1) {
			len = pos - start;
			try {
				tr = Text.decode(value.getBytes(), start, len).trim().split(" ");
				for(String token: tr){
					keyOut.set(token);
					try {
						context.write(keyOut, valueOut);
					} catch (InterruptedException ex) {
						Logger.getLogger(Map1.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
					}
				}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
			start = pos + 1;
			if (start >= value.getLength()) {
				endBlock = true;
				break;
			}
		}
		// pegar a ultima transação, caso tenha
		if (!endBlock) {
			len = value.getLength() - start;
			try {
				tr = Text.decode(value.getBytes(), start, len).split(" ");
				for(String token: tr){
					keyOut.set(token);
					try {
						context.write(keyOut, valueOut);
					} catch (InterruptedException ex) {
						Logger.getLogger(Map1.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
					}
				}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
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
