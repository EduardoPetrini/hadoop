/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.combiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author hadoop
 */
public class Combiner1 extends Reducer<IntWritable, Text, IntWritable, Text>{
    
    Log log = LogFactory.getLog(Combiner1.class);
    SequenceFile.Writer writer;
    double support = 1;
    
    public ArrayList<Integer>  sortValue(Iterable<Text> values){
        
        ArrayList<Integer> aux = new ArrayList();
        
        for (Iterator<Text> it = values.iterator(); it.hasNext();) {
            
            aux.add(Integer.parseInt(it.next().toString()));
        }

        Collections.sort(aux);
        
        return aux;
    }
    
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context){
        
        ArrayList<Integer> v = sortValue(values);
        
        Text out = new Text();
        
        for(Integer i: v){
            out.set(String.valueOf(i));
            try {
                context.write(key, out);
            } catch (    IOException | InterruptedException ex) {
                Logger.getLogger(Combiner1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
}
