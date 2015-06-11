package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import main.Main;

public class AprioriUtils {
	public static int k;
	/**
	 * A partir do arquivo de saída, com 1-itemset por linha, gera-se 2-itemset
	 */
	public static void generate2ItemsetCandidates(){
		String inputFile = Main.fileSequenceOutput+Main.countDir;
		ArrayList<String> itemsets = MrUtils.readSequenfileInHDFS(inputFile);
		Collections.sort(itemsets,NUMERIC_ORDER);
		ArrayList<String> itemset2k = get2itemset(itemsets);
		MrUtils.saveSequenceInHDFS(itemset2k, Main.fileSequenceInput+Main.countDir);
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
	
	public static boolean gerateDynamicKItemsets(String inputFile){
//		k = Main.k;
		boolean success = true;
//		String inputFile = Main.fileCached+Main.countDir;
		ArrayList<String> itemsets = MrUtils.readSequenfileInHDFS(inputFile);
		ArrayList<String> newItemsets = new ArrayList<String>();
		ArrayList<String> tmp1 = new ArrayList<String>();
		ArrayList<String> tmp2 = new ArrayList<String>();
		int lkSize = itemsets.size();
		int ct;
		int cSetSize;
		if(!checkItemsetArray(itemsets)){
			return false;
		}
		
//		if(Main.earlierTime >= 60){
		
//        	ct = lkSize * 1;
//        }else{
        	ct = (int)Math.round(lkSize * 1.2);
//        }
		int mink = k;
		roundGeneration(itemsets, newItemsets);
		k++;
		if(!checkItemsetArray(newItemsets)){
			return false;
		}
		cSetSize = newItemsets.size();
		tmp1.addAll(newItemsets);
		while( cSetSize <= 240){
			
			roundGeneration(tmp1, tmp2);
			k++;
			if(tmp2.size() <= 0){
				k--;
				success = false;
				break;
			}
			tmp1.clear();
			tmp1.addAll(tmp2);
			newItemsets.addAll(tmp2);
			tmp2.clear();
			roundGeneration(tmp1, tmp2);
			k++;
			if(tmp2.size() <= 0){
				k--;
				success = false;
				break;
			}
			tmp1.clear();
			tmp1.addAll(tmp2);
			newItemsets.addAll(tmp2);
			tmp2.clear();
			
			cSetSize += newItemsets.size();
		}

		saveItemsets(newItemsets);
		return success;
	}
	
	private static void saveItemsets(ArrayList<String> itemsets ){
		ArrayList<String> kitemset = new ArrayList<String>();
		for(String item: itemsets){
			if(item.split(" ").length == k){
				kitemset.add(item);
			}
		}
		
		MrUtils.saveSequenceInHDFS(kitemset, "/user/eduardo/tmp/"+k+"itemset");
		MrUtils.saveSequenceInHDFS(itemsets, "/user/eduardo/tmp/"+k+"."+k+"itemset");
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
    	if(k == 1) return true;
    	for(int a = 0; a < k - 1; a++){
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
