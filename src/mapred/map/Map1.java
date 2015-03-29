/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.map;

import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.MapContextImpl;

import app.PrefixTree;


/**
 *
 * @author eduardo
 */
public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map1.class);
    IntWritable count = new IntWritable(1);
    Text keyOut = new Text();
    double support;
    
    ArrayList<String> candidates;
    ArrayList<String> tempCandidates;
    HashMap<String, Integer> itemSup;
    
    ArrayList<String[]> transactions;
    PrefixTree prefixTree;
    
    String splitId;
    
    @Override
    public void setup(Context context){
    	String sup = context.getConfiguration().get("support");
    	
    	support = Double.parseDouble(sup);
    	log.info("Iniciando Map 1...");
    	
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException{
    	//Recebe todo o bloco de transações de uma vez
    	//Aplica-se o algoritmo Apriori
    	
    	//buildTransactionsArraySet
    	candidates = new ArrayList<String>();
    	prefixTree = new PrefixTree(0);
    	transactions = new ArrayList<String[]>();
    	buildTransactionsArraySet(value.toString());
    	
    	/*Obter o id do split para ser usado na equação da função Reduce*/
    	setSplitId(context);
    	
    	int k = 1;
    	String[] itemset;
    	int itemsetIndex;
    	do{
    		generateCandidates(k, context);
    		//Verificar existência e contar o support de cada itemset
    		if(k > 1){
    			for(String[] transaction: transactions){
    				if(transaction.length >= k){
	    				for(int i = 0; i < transaction.length; i++){
	    					itemset = new String[k];
	    					itemsetIndex = 0;
	    					subSet(transaction, prefixTree, i, k, itemset, itemsetIndex);
	    				}
    				}
    			}
    		}
    		
    		k++;
    	}while(candidates.size() > 1);
    	
    	//Envia os elementos da hashMap para o reduce com seu respectivo suporte parcial
    	Set<String> keys = itemSup.keySet();
    	Text keyOut = new Text();
    	IntWritable valueOut = new IntWritable();
    	Integer v;
    	for(String localKeys: keys){
    		v = itemSup.get(localKeys);
    		if(v != null){
    			keyOut.set(localKeys);
    			valueOut.set(v);
    			try {
					context.write(keyOut, valueOut);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
    	
    }
    
    public void setSplitId(Context context){
    	//context.getCounter(MapContextImpl.SPLIT_FILE);
    	System.out.println("MAP_INPUT_RECORDS: "+context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
    	System.out.println("TOTAL_LAUNCHED_MAPS: "+context.getCounter(JobCounter.TOTAL_LAUNCHED_MAPS).getValue());
    	System.out.println("SPLIT_RAW_BYTES: "+context.getCounter(TaskCounter.SPLIT_RAW_BYTES).getValue());
//    	System.out.println("BYTES_READ: "+context.getCounter(FileSystemCounter.BYTES_READ).getValue());
    	FileSplit fs = new FileSplit();
    	System.out.println("File Split len: "+fs.getLength());
    	System.out.println("File Split start: "+fs.getStart());
    	System.out.println("File Split path: "+fs.getPath());
    }
    
    /**
     * 
     * @param transaction
     * @param pt
     * @param i
     * @param k
     * @param itemset
     * @param itemsetIndex
     */
    private void subSet(String[] transaction, PrefixTree pt, int i,
			int k, String[] itemset, int itemsetIndex) {
    	if(i >= transaction.length){
			return;
		}
		int index = pt.getPrefix().indexOf(transaction[i]);
		
		if(index == -1){
			return;
		}else{
			itemset[itemsetIndex] = transaction[i];
			/*if(i == transaction.length-1 && i == k-1){
				StringBuilder sb = new StringBuilder();
				System.out.println("Achou1 "+Arrays.asList(itemset));
				for(String s: itemset){
					if(s != null)
						sb.append(s).append(" ");
				}
				
				itemset[itemsetIndex] = "";
				addToHashItemSup(sb.toString().trim());
				return;
			}else{*/
				i++;
				if(pt.getLevel() == k-1){
					StringBuilder sb = new StringBuilder();
					System.out.println("Achou2 "+Arrays.asList(itemset));
					for(String s: itemset){
						if(s != null)
							sb.append(s).append(" ");
					}
					addToHashItemSup(sb.toString().trim());
					itemset[itemsetIndex] = "";
					return;
				}
				if(pt.getPrefixTree().isEmpty()){
					itemset[itemsetIndex] = "";
					return;
				}else{
					itemsetIndex++;
					while(i < transaction.length){
						subSet(transaction, pt.getPrefixTree().get(index),i, k, itemset, itemsetIndex);
						i++;
					}
				}
//			}
		}
	}
	public void generateCandidates(int n, Context context){
    	ArrayList<String> tempCandidates = new ArrayList<String>(); 
    	
    	String str1, str2; 
    	StringTokenizer st1, st2;
    	String tmpItem;
    	String[] tmpItemsets;
    			
    	if(n==1){
    		itemSup = new HashMap<String, Integer>();
    		for(int i=0; i<transactions.size(); i++){
    			tmpItemsets = transactions.get(i);
    			for(int j = 0; j < tmpItemsets.length; j++){
    				addToHashItemSup(tmpItemsets[j]);
					if(!tempCandidates.contains(tmpItemsets[j])){
						tempCandidates.add(tmpItemsets[j]);		
					}
    			}
    		}
    		tempCandidates.removeAll(removeUnFrequentItems(tempCandidates));
    		Collections.sort(tempCandidates, ALPHABETICAL_ORDER);
    		
    		//envia para o reduce
    		outputToReduce(tempCandidates, context);
    		itemSup.clear();
    	}
    	else if(n==2) {
    		for(int i=0; i<candidates.size(); i++){
    			st1 = new StringTokenizer(candidates.get(i));
    			str1 = st1.nextToken();
    			for(int j=i+1; j<candidates.size(); j++)		{
    				st2 = new StringTokenizer(candidates.get(j));
    				str2 = st2.nextToken();
    				tmpItem = str1+" "+str2;
    				tempCandidates.add(tmpItem);
    				System.out.println("Adicionando na hash "+tmpItem);
    				prefixTree.add(prefixTree,tmpItem.split(" "),0);
    			}
    		}
    	}else{

    		for(int i=0; i<candidates.size(); i++){

    			for(int j=i+1; j<candidates.size(); j++){

    				str1 = new String();
    				str2 = new String();

    				st1 = new StringTokenizer(candidates.get(i));
    				st2 = new StringTokenizer(candidates.get(j));

    				for(int s=0; s<n-2; s++){
    					str1 = str1 + " " + st1.nextToken();
    					str2 = str2 + " " + st2.nextToken();
    				}

    				if(str2.compareToIgnoreCase(str1)==0){
    					tmpItem = (str1 + " " + st1.nextToken() + " " + st2.nextToken()).trim();
    					tempCandidates.add(tmpItem);
    					//add to prefixTree
    					System.out.println("Adicionando na hash "+tmpItem);
    					prefixTree.add(prefixTree,tmpItem.split(" "),0);
    				}
    			}
    		}
    	}
    	candidates.clear();
    	candidates = new ArrayList<String>(tempCandidates);
    	tempCandidates.clear();
    }
	
	/**
	 * Envia os itemsets parciais para o reduce
	 * @param arrayCandidates
	 * @param context
	 */
	private void outputToReduce(ArrayList<String> arrayCandidates, Context context) {
		Integer value;
		Text key = new Text();
		IntWritable val = new IntWritable();
		
		for(String item: arrayCandidates){
			value = itemSup.get(item);
			key.set(item);
			val.set(value);
			try {
				context.write(key , val);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
    
    public void buildTransactionsArraySet(String block){
    	String[] transaction = block.split("\n");
    	
    	for(String tr : transaction){
    		transactions.add(tr.split(" "));    		
    	}
    }
    
    public void addToHashItemSup(String item){
    	Integer value = 0;
    	
    	if((value = itemSup.get(item)) == null){
    		itemSup.put(item, 1);
    	}else{
    		value++;
    		itemSup.put(item, value);
    	}
    }
    
    public ArrayList<String> removeUnFrequentItems(ArrayList<String> tempCandidates){
    	Integer value;
    	ArrayList<String> rmItems = new ArrayList<String>();
    	for(String item: tempCandidates){
    		value = itemSup.get(item);
    		if(value == null || value < support){
    			rmItems.add(item);
    		}
    	}
    	
    	return rmItems;
    }
    
    @Override
    public void cleanup(Context context){
    	context.getConfiguration().set("totalTransactions", String.valueOf(transactions.size()));
    }
    
    private static Comparator<Object> ALPHABETICAL_ORDER = new Comparator<Object>()  {
		public int compare(Object ob1, Object ob2) {
			String name1 = "";
			String name2 = "";
			Collator collator = Collator.getInstance(Locale.getDefault());
	        collator.setStrength(Collator.PRIMARY);
			
			return collator.compare(name1, name2);
		}
	};
}
