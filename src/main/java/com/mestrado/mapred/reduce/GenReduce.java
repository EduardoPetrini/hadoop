/*

 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.reduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author eduardo
 */
public class GenReduce extends Reducer<Text, Text, Text, Text>{
    
    private Log log;
    private Text valueOut;
    private SequenceFile.Writer writer;
    private Text keyOut;
    private ArrayList<String> suffix;
    
    @Override
    public void setup(Context context) throws IOException{
        log = LogFactory.getLog(GenReduce.class);
        log.info("Iniciando o reduce para a geração de candidatos");
        keyOut = new Text();
        valueOut = new Text("1");
        suffix = new ArrayList<String>();
        String outputL = context.getConfiguration().get("outputL");
        Path path = new Path(outputL);
        
        writer = SequenceFile.createWriter(context.getConfiguration(), SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
    	for (Iterator<Text> it = values.iterator(); it.hasNext();) {
    		suffix.add(it.next().toString());
        }
    	String prefix;
    	for(int i = 0; i < suffix.size()-1; i++){
    		prefix = key.toString()+" "+suffix.get(i)+" ";
    		for(int j = i+1; j < suffix.size(); j++){
    			try {
    				keyOut.set(prefix+suffix.get(j));
    				context.write(keyOut, valueOut);
    				saveInCache(keyOut, valueOut);
    			} catch (IOException | InterruptedException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
        	}
    	}
    }
       
    public void saveInCache(Text key, Text value){
    	try {
            writer.append(key, value);
        } catch (IOException ex) {
            Logger.getLogger(GenReduce.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE para a geração de candiadtos");
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
