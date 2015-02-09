/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

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
    
    Log log = LogFactory.getLog(Map1.class);
    IntWritable count = new IntWritable(1);
    Text keyOut = new Text();
    
    @Override
    public void setup(Context context){
    	log.info("Iniciando Map 1...");
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException{
    	System.out.println("Print value:");
        System.out.println(value.toString());
        StringTokenizer token = new StringTokenizer(value.toString());
        while(token.hasMoreTokens()){
        	keyOut.set(token.nextToken());
            try {
                context.write(keyOut, count);
            } catch (InterruptedException ex) {
                Logger.getLogger(Map1.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }
}
