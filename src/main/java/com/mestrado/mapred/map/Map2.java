/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.util.ArrayList;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import main.java.com.mestrado.app.HashTree;

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
    
    private Log log = LogFactory.getLog(Map2.class);
    private IntWritable countOut = new IntWritable(1);
    private SequenceFile.Reader reader;
    private ArrayList<String> fileCached;
    private HashPrefixTree hpt;
    private int k;
    private Text keyOut;
    private IntWritable valueOut;
    
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String inputCand = context.getConfiguration().get("inputCandidates");
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        
        log.info("AprioriCpa Map contagem de C"+k);
        log.info("Arquivo de entrada no inputCandidates = "+inputCand);
        
        Path path = new Path(inputCand);
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        
        hpt = new HashPrefixTree();
        openFile(context);
        
        keyOut = new Text();
        valueOut = new IntWritable(1);
    }
    
    /**
     * 
     * @param transaction
     * @param hNode
     * @param i
     * @param k
     * @param itemset
     * @param itemsetIndex
     * @param context
     */
    private void subSet(String[] transaction, HashNode hNode, int i,
			String[] itemset, int itemsetIndex, Context context) {
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
				subSet(transaction, son, i, itemset, itemsetIndex, context);
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
    		itemset = new String[k];
    		subSet(transaction, hpt.getHashNode(), i, itemset, 0,context);
    	}
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
				hpt.add(hpt.getHashNode(),key.toString().split(" "),0);
	        }
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
    }
}
