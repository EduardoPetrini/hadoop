/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.combiner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author hadoop
 */
public class CombinerGen extends Reducer<Text, Text, Text, Text>{
    
	private Log log = LogFactory.getLog(CombinerGen.class);
	private SequenceFile.Writer writer;
    private ArrayList<String> suffix;
    private Text valueOut;
    private Text keyOut;
    private HashSet<String> freqItemsets;
    
    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context){
    	valueOut = new Text();
    	suffix = new ArrayList<String>();
    	keyOut = new Text();
    	
    	/**
         * Ler o arquivo de entrada que contém Lk-1
         */
        String inputGen = context.getConfiguration().get("inputFileToGen");
        Path inPath = new Path(inputGen);
        readFileToHash(context, inPath);
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
    	suffix.clear();
    	for (Iterator<Text> it = values.iterator(); it.hasNext();) {
    		suffix.add(it.next().toString());
        }
    	if(suffix.size() > 1){
    		Collections.sort(suffix, NUMERIC_ORDER);
        
	    	String prefix;
	    	String newItemset;
	    	int count = 0;
	    	for(int i = 0; i < suffix.size()-1; i++){
	    		prefix = key.toString()+" "+suffix.get(i)+" ";
	    		for(int j = i+1; j < suffix.size(); j++){
	    			newItemset = prefix+suffix.get(j);
	    			
	    			if(allSubsetIsFrequent(newItemset.split(" "))){
	    				keyOut.set(newItemset);
	    				count++;
	    				try {
	    					valueOut.set("#");
							context.write(keyOut, valueOut);
						} catch (IOException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	    			}
	        	}
	    	}
    	}else{
	    	try{
	    		valueOut.set(suffix.get(0));
	    		context.write(key, valueOut);
	    	} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
    	}
    }

	private boolean allSubsetIsFrequent(String[] itemset){
    	int indexToSkip = 0;
		StringBuilder subItem;
		for(int j = 0; j < itemset.length-1; j++){
			subItem = new StringBuilder();
			for(int i = 0; i < itemset.length; i++){
				if(i != indexToSkip){
					subItem.append(itemset[i]).append(" ");
				}
			}
			//subItem gerado, verificar se é do conjunto frequente
			
			if(!freqItemsets.contains(subItem.toString().trim())){
				return false;
			}
			indexToSkip++;
		}
		
		return true;
    }
	
	private void readFileToHash(Context context, Path inPath){
    	freqItemsets = new HashSet<String>();
    	
    	try {
        	
			FileSystem fs = inPath.getFileSystem(context.getConfiguration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(inPath)));
			String line;
			while ((line = br.readLine()) != null){
				freqItemsets.add(line.split("\\t")[0].trim());
			}
			br.close();
        }catch(IOException e){
        	e.printStackTrace();
        }
    }
    
    public static Comparator<String> NUMERIC_ORDER = new Comparator<String>() {
    	public int compare(String obj1, String obj2){
    		
    		int o1 = Integer.parseInt(obj1.trim());
    		int o2 = Integer.parseInt(obj2.trim());
    		
			if(o1 < o2){
				return -1;
			}else if(o1 > o2){
				return 1;
			}
    		return 0;
    	}
	};
    
}
