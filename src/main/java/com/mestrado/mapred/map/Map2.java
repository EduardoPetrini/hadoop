/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Gerar itemsets de tamanho 2.
 * @author eduardo
 */
public class Map2  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map2.class);
    IntWritable countOut = new IntWritable(1);
    SequenceFile.Reader reader;
    ArrayList<String> fileCached;
    HashPrefixTree hpt;
    int k;
    private Text keyOut;
    private IntWritable valueOut;
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        String fileCachedRead = context.getConfiguration().get("fileCachedRead");
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        
        log.info("AprioriDpc Map 2");
        log.info("Arquivo Cached = "+fileCachedRead);
        URI[] patternsFiles = context.getCacheFiles();
        
        Path path = new Path(patternsFiles[0].toString());
        
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        openFile(fileCachedRead, context);
        
        //Gerar combinações dos itens de acordo com k
        
        hpt = new HashPrefixTree();
        //System.out.println("K is "+k);
        String[] itemsetC = new String[2];
        for (int i = 0; i < fileCached.size(); i++){
        	itemsetC[0] = fileCached.get(i);
        	for (int j = i+1; j < fileCached.size(); j++){
        		itemsetC[1] = fileCached.get(j);
        		hpt.add(hpt.getHashNode(),itemsetC,0);
        	}
        }
        keyOut = new Text();
        valueOut = new IntWritable(1);
    }
    
    /**
     * 
     * @param itemA
     * @param itemB
     * @param i
     * @param j
     * @return
     */
    public boolean isSamePrefix(String[] itemA, String[] itemB, int i, int j){
    	if(k == 2) return true;
    	for(int a = 0; a < k -2; a++){
            if(!itemA[a].equals(itemB[a])){
            	//System.out.println("Não é o mesmo prefixo: "+itemA[a]+" != "+itemB[a]+"  "+itemA+" "+itemB);
                return false;
            }
        }
        
    	return true;
    }
    
    /**
     * 
     * @param itemA
     * @param itemB
     * @return
     */
    public String[] combine(String[] itemA, String[] itemB){
        String[] item = new String[itemB.length+1];
        
        for(int i = 0; i < itemA.length; i++){
            item[i] = itemA[i];
        }
        item[item.length-1] = itemB[itemB.length-1];
        return item;
    }
    
    /**
    /**
     * 
     * @param transaction
     * @param pt
     * @param i
     * @param k
     * @param itemset
     * @param itemsetIndex
     */
    private void subSet(String[] transaction, HashNode hNode, int i,
			int k, String[] itemset, int itemsetIndex, Context context) {
    	if(i >= transaction.length){
			return;
		}
		
		HashNode son = hNode.getHashNode().get(transaction[i]);
		
		if(son == null){
			return;
		}else{
			itemset[itemsetIndex] = transaction[i];
			
			if(hNode.getLevel() == k-1){
				StringBuilder sb = new StringBuilder();
				for(String item: itemset){
					if(item != null){
						sb.append(item).append(" ");
					}
				}
				// System.out.println("Encontrou: "+sb.toString().trim());
				keyOut.set(sb.toString().trim());
				try {
					context.write(keyOut, valueOut);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				itemset[itemsetIndex] = "";
				return;
			}
			
			i++;
			itemsetIndex++;
			while(i < transaction.length){
				subSet(transaction, son, i, k, itemset, itemsetIndex, context);
				for(int j = itemsetIndex; j < itemset.length; j++){
					itemset[j] = "";
				}
				i++;
			}
		}
	}
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	
		//Aplica a função subset e envia o itemset para o reduce
    	String[] transaction = value.toString().split(" ");
    	String[] itemset;
    	for(int i = 0; i < transaction.length; i++){
    		itemset = new String[2];
    		subSet(transaction, hpt.getHashNode(), i, 2,itemset, 0,context);
    		
    	}
    }
    
    /**
     * 
     * @param path
     * @param context
     * @return
     */
    public void openFile(String path, Context context){
    	fileCached = new ArrayList<String>();
    	try {
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				//System.out.println("Add Key: "+key.toString());
	            fileCached.add(key.toString());
	        }
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	Collections.sort(fileCached, NUMERICAL_ORDER);
    }
    
    private static Comparator<Object> NUMERICAL_ORDER = new Comparator<Object>()  {
		public int compare(Object ob1, Object ob2) {
			int val1 = Integer.parseInt((String)ob1);
			int val2 = Integer.parseInt((String)ob2);
			
			return val1 > val2? 1: val1 < val2? -1 : 0;
		}
	};
}
