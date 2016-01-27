/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

/**
 * Count itemsets by String List K >= 2.
 * 
 * @author eduardo
 */
public class Map3Spark implements FlatMapFunction<Tuple2<String,Iterable<String>>, String[]>{
	
	private Map<String,Integer> kLessOneItemsets;
	
	public Map3Spark(Map<String,Integer> kLessOneItemsets){
		this.kLessOneItemsets = kLessOneItemsets;
	}

	@Override
	public Iterable<String[]> call(Tuple2<String, Iterable<String>> t) throws Exception {
		List<String> suffix = new ArrayList<String>((Collection<? extends String>) t._2);
		Collections.sort(suffix,NUMERIC_ORDER);
		String prefix;
    	String[] newItemset;
    	List<String[]> newItemsets = new ArrayList<String[]>();
		for(int i = 0; i < suffix.size()-1; i++){
    		prefix = t._1+" "+suffix.get(i)+" ";
    		for(int j = i+1; j < suffix.size(); j++){
    			newItemset = (prefix+suffix.get(j)).split(" ");
    			
    			if(allSubsetIsFrequent(newItemset)){
    				newItemsets.add(newItemset);
    			}
        	}
    	}
		return newItemsets;
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
			if(kLessOneItemsets.get(subItem.toString().trim()) == null){
				return false;
			}
			indexToSkip++;
		}
		return true;
    }
	
	public static Comparator<String> NUMERIC_ORDER = new Comparator<String>(){

		@Override
		public int compare(String o1, String o2) {
			Integer item1 = Integer.parseInt(o1.trim());
			Integer item2 = Integer.parseInt(o2.trim());
			return item1.compareTo(item2);
		}
		
	};
}