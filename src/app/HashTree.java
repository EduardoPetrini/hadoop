package app;

import java.util.ArrayList;

public class HashTree {
	private HashTree[] nodes;
	private int k;
	private int level;
	private ArrayList<String> itemsets;
	
	public HashTree(int k) {
		nodes = new HashTree[9];
		this.level = 0;
		this.k = k;
	}
	public int getK() {
		return k;
	}
	public void setK(int k) {
		this.k = k;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	
	public HashTree[] getNodes() {
		return nodes;
	}
	public void setNodes(HashTree[] nodes) {
		this.nodes = nodes;
	}
	
	public HashTree getHasTreeNode(int index){
		return nodes[index];
	}
	public void add(String itemset){
		String[] item = itemset.split(" ");
		int hash;
		HashTree tmp = this;
		for(int i = 0; i < item.length; i++){
			hash = Integer.parseInt(item[i]) % 9;
			if(tmp.nodes[hash] == null){
				tmp.nodes[hash] = new HashTree(tmp.k);
				tmp.nodes[hash].setLevel(tmp.level+1);
			}
			tmp = tmp.nodes[hash];
		}
		if(tmp != this){
			//Inserir o itemset na folha da hashTree
			if(tmp.itemsets != null && 
					!tmp.itemsets.contains(itemset)){
				tmp.itemsets.add(itemset);
			}else if(tmp.itemsets == null){
				tmp.itemsets = new ArrayList<String>();
				tmp.itemsets.add(itemset);
			}
		}
	}
	
	public void find(String t){
		String[] ts = t.split(" ");
		
		for(int i = 0; i < ts.length; i++){
			int hash = Integer.parseInt(ts[i]) % 9;
			HashTree tmp = this;
			int count = 1;
			int j = i;
			while(count <= k){
				if(tmp.nodes[hash] != null){
					tmp = nodes[hash];
					j++;
					if(j < ts.length){
						hash = Integer.parseInt(ts[j]) % 9;
					}
				}else if(!tmp.itemsets.isEmpty()){
					System.out.println("Tem itemset");
				}
			}
		}
	}
	
	public static void main(String[] args){
		String[] itemsets = {"1 2", "1 3", "1 4", "2 3", "2 4", "3 4"};
		String t = "1 2 3 4";
		HashTree hashTree = new HashTree(2);
		
		for(String itemset : itemsets){
			hashTree.add(itemset);
		}
		
		
	}
}
