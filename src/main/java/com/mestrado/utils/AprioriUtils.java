package main.java.com.mestrado.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import main.java.com.mestrado.main.Main;

public class AprioriUtils {
	public static int maxk;
	public static int mink;
	/**
	 * A partir do arquivo de saída, com 1-itemset por linha, gera-se 2-itemset
	 */
	public static long generate2ItemsetCandidates(){
		String inputFile = Main.user+"output"+Main.countDir;
		ArrayList<String> itemsets = MrUtils.readAllFromHDFSDir(inputFile);
		long ini = System.currentTimeMillis();
		Collections.sort(itemsets,NUMERIC_ORDER);
		ArrayList<String> itemset2k = get2itemset(itemsets);
		long fim = System.currentTimeMillis();
		MrUtils.saveSequenceInHDFS(itemset2k, Main.inputCandidates+(Main.countDir+1));
		Main.candFilesNames = new ArrayList<String>();
		Main.candFilesNames.add(Main.inputCandidates+(Main.countDir+1));
		if(itemset2k.size() == 0){
			return -1;
		}
		return (fim - ini)/1000;
	}
	
	private static ArrayList<String> get2itemset(ArrayList<String> itemset){
		ArrayList<String> newItemsets = new ArrayList<String>();
		
		for(int i = 0; i < itemset.size(); i++){
			for(int j = i+1; j < itemset.size(); j++){
				newItemsets.add(itemset.get(i)+" "+itemset.get(j));
			}
		}
		
		return newItemsets;
	}
	
	public static void roundGeneration(ArrayList<String> itemsets, ArrayList<String> newItemsets){
		String[] itemA;
        String[] itemB;
        String itemsetCandidate;
        for (int i = 0; i < itemsets.size()-1; i++){
        	for (int j = i+1; j < itemsets.size(); j++){
        		itemA = itemsets.get(i).split(" ");
        		itemB = itemsets.get(j).split(" ");
        		if(isSamePrefix(itemA, itemB, i, j)){
        			itemsetCandidate = combine(itemA, itemB);
        			newItemsets.add(itemsetCandidate);
        		}
        	}
        }
	}
	
	 /**
     * 
     * @param itemA
     * @param itemB
     * @return
     */
    public static String combine(String[] itemA, String[] itemB){
        StringBuilder sb = new StringBuilder();
        
        for(int i = 0; i < itemA.length; i++){
            sb.append(itemA[i]).append(" ");
        }
        sb.append(itemB[itemB.length-1]);
        return sb.toString();
    }
    
	 /**
     * 
     * @param itemA
     * @param itemB
     * @param i
     * @param j
     * @return
     */
    public static boolean isSamePrefix(String[] itemA, String[] itemB, int i, int j){
    	if(maxk == 1) return true;
    	for(int a = 0; a < maxk - 1; a++){
    		try{
	            if(!itemA[a].equals(itemB[a])){
	                return false;
	            }
    		}catch(ArrayIndexOutOfBoundsException e){
    			System.out.println(itemA);
    			System.out.println(itemB);
    			System.out.println(a);
    			e.printStackTrace();
    		}
        }
        
    	return true;
    }
    
	private static boolean checkItemsetArray(ArrayList<String> itemsets) {
		if(itemsets != null && itemsets.size() > 0){
			return true;
		}
		return false;
	}


	public static Comparator<Object> ITEMSET_NUMERIC_ORDER = new Comparator<Object>() {
    	public int compare(Object obj1, Object obj2){
    		
    		String[] o1 = ((String)obj1).trim().split(" ");
    		String[] o2 = ((String)obj2).trim().split(" ");
    		int a;
    		int b;
    		for(int i = 0; i < o1.length; i++){
    			a = Integer.parseInt(o1[i]);
    			b = Integer.parseInt(o2[i]);
    			
    			if(a < b){
    				return -1;
    			}else if(a > b){
    				return 1;
    			}else{
    				continue;
    			}
    		}
    		
    		return 0;
    	}
	};

	public static Comparator<Object> NUMERIC_ORDER = new Comparator<Object>() {
    	public int compare(Object obj1, Object obj2){
    		
    		int o1 = Integer.parseInt(((String)obj1).trim());
    		int o2 = Integer.parseInt(((String)obj2).trim());
    		
			if(o1 < o2){
				return -1;
			}else if(o1 > o2){
				return 1;
			}
    		return 0;
    	}
	};
}
