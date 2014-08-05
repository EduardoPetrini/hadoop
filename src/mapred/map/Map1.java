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
public class Map1 extends Mapper<LongWritable, Text, IntWritable, Text>{
    
    Log log = LogFactory.getLog(Map1.class);
    int size = 0;
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException{
        
        StringTokenizer token = new StringTokenizer(value.toString());
        String tid = token.nextToken();
        String t;
        
        while(token.hasMoreTokens()){
            t = token.nextToken();
            
            try {
                context.write(new IntWritable(Integer.parseInt(t)), new Text(tid));
            } catch (InterruptedException ex) {
                Logger.getLogger(Map1.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
            }
        }
    }
}
