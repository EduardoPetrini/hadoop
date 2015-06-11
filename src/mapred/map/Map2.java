/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import app.HashTree;

/**
 * Gerar itemsets de tamanho 2.
 * @author eduardo
 */
public class Map2  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map2.class);
    IntWritable countOut = new IntWritable(1);
    SequenceFile.Reader reader;
    ArrayList<String> fileCached;
    HashTree hashTree;
    int k;
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        String fileSequenceInput = context.getConfiguration().get("fileSequenceInput");
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        
        log.info("Iniciando map 2v2 count = "+count);
        log.info("Arquivo Cached = "+fileSequenceInput);
        
        Path path = new Path(fileSequenceInput);
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        
        hashTree = new HashTree(k);
        openFile(context);
    }
    
    /**
     * 
     * @param transaction
     * @param hasht
     * @param i
     * @param itemset
     * @param context
     */
    public void subset(String[] transaction, HashTree hasht, int i, ArrayList<String> itemset,Context context){
    	if(hasht == null){
			return;
		}
		
		if(hasht.getLevel() > hasht.getK()){
//			System.out.println("\nAchou -> Itemset: "+itemset.toString());
			try{
				context.write(new Text(itemset.toString()), new IntWritable(1));
			}catch(IOException | InterruptedException e){
				e.printStackTrace();
			}
			return;
		}
		
		if(i >= transaction.length){
			return;
		}
		
		while(i < transaction.length){
			int hash = Integer.parseInt(transaction[i]) % 9;
			
			if(hasht.getNodes()[hash] != null){
				itemset.add(transaction[i]);
				subset(transaction, hasht.getNodes()[hash], i+1, itemset, context);
				itemset.remove(itemset.size()-1);
			}
			i++;
		}
		
		return;
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	
		//Aplica a função subset e envia o itemset para o reduce
    	ArrayList<String> itemset = new ArrayList();
		subset(value.toString().split(" "), hashTree, 0, itemset, context);
    }
    
    /**
     * 
     * @param path
     * @param context
     * @return
     */
    public void openFile(Context context){
    	try {
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				//System.out.println("Add Key: "+key.toString());
//	            fileCached.add(key.toString());
				hashTree.add(key.toString());
	        }
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
