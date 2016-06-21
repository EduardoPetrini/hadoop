/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author eduardo
 */
public class GenMap extends Mapper<LongWritable, Text, Text, Text>{
    
    private Log log;
    private Text valueOut;
    private Text keyOut;
    
    @Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
    	log = LogFactory.getLog(GenMap.class);
    	valueOut = new Text();
    	keyOut = new Text();
    	
    	log.info("AprioriCpa Map geração");
	}

	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException{
		String[] tokens;
		boolean endBlock = false;
		int pos;
		int start = 0;
		int len;
		StringBuilder sb;

		while ((pos = value.find("\n", start)) != -1) {
			len = pos - start;
			try {
				tokens = Text.decode(value.getBytes(), start, len).trim().split(" ");
				sb = new StringBuilder();
		        for(int i = 0; i < tokens.length-1; i++){
		        	sb.append(tokens[i]).append(" ");
		        }
		    	keyOut.set(sb.toString().trim());
		    	valueOut.set(tokens[tokens.length-1].split("\\t")[0]);//split para ignorar a frequencia
		        try {
		            context.write(keyOut, valueOut);
		        } catch (InterruptedException ex) {
		            Logger.getLogger(GenMap.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
		            log.error(ex.getMessage());
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
				tokens = Text.decode(value.getBytes(), start, len).trim().split(" ");
				sb = new StringBuilder();
		        for(int i = 0; i < tokens.length-1; i++){
		        	sb.append(tokens[i]).append(" ");
		        }
		    	keyOut.set(sb.toString().trim());
		    	valueOut.set(tokens[tokens.length-1].split("\\t")[0]);//split para ignorar a frequencia
		        try {
		            context.write(keyOut, valueOut);
		        } catch (InterruptedException ex) {
		            Logger.getLogger(GenMap.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
		            log.error(ex.getMessage());
		        }
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
    }
}