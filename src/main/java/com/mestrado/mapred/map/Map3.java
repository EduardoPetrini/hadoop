/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;

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
 * Gerar itemsets de tamanho k, k+1 e k+3 em uma única instância Map/Reduce.
 * @author eduardo
 */
public class Map3  extends Mapper<LongWritable, Text, Text, Text>{
    
	private Log log = LogFactory.getLog(Map3.class);
	private IntWritable countOut = new IntWritable(1);
	private SequenceFile.Reader reader;
	private HashPrefixTree hpt;
	private int mink, maxk;
    private Text keyOut;
    private Text valueOut;
    
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        String fileSequenceInput = context.getConfiguration().get("fileSequenceInput");
        maxk = Integer.parseInt(context.getConfiguration().get("maxk"));
        mink = Integer.parseInt(context.getConfiguration().get("mink"));
        
        log.info("Iniciando map 3 count = "+count);
        log.info("Arquivo Cached = "+fileSequenceInput);
        
        Path path = new Path(fileSequenceInput);
        
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        openFile(fileSequenceInput, context);
        
        System.out.println("MinK "+mink+" maxK "+maxk);
        keyOut = new Text();
        valueOut = new Text(maxk+":"+1);
    }
    
    /**
     * 
     * @param transaction
     * @param hNode
     * @param i
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
			if(son.getLevel() > maxk) return;
			itemset[itemsetIndex] = transaction[i];
			
			if(son.getLevel() >= mink){
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
    	
    	String[] transaction = value.toString().split(" ");
    	String[] itemset;

    	if(transaction.length >= mink){
    		for(int i = 0; i < transaction.length; i++){
    			itemset = new String[maxk];
    			subSet(transaction, hpt.getHashNode(), i, itemset, 0, context);
    		}
    	}
    }
    
    /**
     * 
     * @param path
     * @param context
     * @return
     */
    public void openFile(String path, Context context){
    	try {
			hpt =  new HashPrefixTree();
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				hpt.add(hpt.getHashNode(), key.toString().split(" "), 0);
	        }
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}
