package app;

import java.util.ArrayList;

public class HashTree {
	private HashTree[] nodes;
	private int k;
	private int level;
	private ArrayList<String> itemsets;
	
	public HashTree(int k) {
		nodes = new HashTree[9];
		this.level = 1;
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
			tmp.setLevel(k+1);
			tmp.setK(k);
			if(tmp.itemsets != null && 
					!tmp.itemsets.contains(itemset)){
				tmp.itemsets.add(itemset);
			}else if(tmp.itemsets == null){
				tmp.itemsets = new ArrayList<String>();
				tmp.itemsets.add(itemset);
			}
		}
	}
	
	public void find(HashTree hashTree, String[] ts, int i, StringBuilder itemset){
		if(hashTree == null) return;
		System.out.println(hashTree.level+" "+hashTree.k+" i: "+i+" itemset "+itemset.toString());
		
		if(i >= ts.length){ return; }
		
		if(hashTree.level > hashTree.k){
			//chegou na folha
			System.out.println("Itemset: "+itemset.toString());
			//Fazer a busca binária
			printItemsets(hashTree.itemsets);
			
			return ;
		}
		
		int hash = Integer.parseInt(ts[i]) % 9;
		
		if(hashTree.nodes[hash] != null){
			itemset.append(ts[i]).append(" ");
			while (i < ts.length-1){
				i++;
				hash = Integer.parseInt(ts[i]) % 9;
				find(hashTree.nodes[hash], ts, i, itemset);
			}
			
		}
//		if(i >= ts.length-1){
//			return false;
//		}
//		find(hashTree, ts, i+1, itemset);
		return;
		
	}
	
	public void findOld(String t){
		String[] ts = t.split(" ");
		
		for_ex:
		for(int i = 0; i < ts.length; i++){
			int firstHash = Integer.parseInt(ts[i]) % 9;
			int hash;
			HashTree firstItem = this;
			int count = 1;
			if(firstItem.nodes[firstHash] != null){
				System.out.println("Levelex: "+firstItem.level);
				HashTree tmp = firstItem.nodes[firstHash];
				for_in:
				for(int j = i+1; j < ts.length; j++){
					hash = Integer.parseInt(ts[j]);
					if(tmp.nodes[hash] != null){
					System.out.println("********************Testado com "+ts[i]+" "+ts[j]+" em teste");
						System.out.println("Levelin: "+tmp.level);
						tmp = tmp.nodes[hash];
						count++;
						if(count == k){
							//Atingiu o limite do k, logo espera-se que tenha chegado em uma folha
							System.out.println("Achou itemset: "+ts[i]+" "+ts[j]);
							printItemsets(tmp.itemsets);
							count--;
							continue for_in;
							
						}
					}{
						System.out.println("Node é null");
					}
				}
			}
//			while(count <= k+1){
//				if(j >= ts.length) break;
//				System.out.println("Count "+k+", level "+tmp.level+", hash "+hash+", item i "+ts[i]+", item j "+ts[j]);
//				HashTree.printNodes(tmp.nodes);
//				if(tmp.nodes[hash] != null){
//					tmp = tmp.nodes[hash];
//					j++;
//					count++;
//					if(j < ts.length){
//						hash = Integer.parseInt(ts[j]) % 9;
//					}
//				}else if(tmp.itemsets != null && !tmp.itemsets.isEmpty()){
//					System.out.println("Tem itemset: "+ts[i]+" "+ts[j]);
//					break;
//				}
//			}
//			if(tmp.itemsets != null && !tmp.itemsets.isEmpty()){
//				System.out.println("Tem itemset");
//			}
		}
	}
	
	private void printItemsets(ArrayList<String> itemsets2) {
		// TODO Auto-generated method stub
		System.out.println("Items no conjunto:");
		for(String i: itemsets2){
			if(i != null && !i.equals("")){
				System.out.print(i+" ");
			}
		}
		System.out.println("");
	}
	public static void printNodes(HashTree[] nodes){
		for(int i = 0; i < 9; i++){
			System.out.print(nodes[i]+" ");
		}
		System.out.println("");
	}
	
	public void printAllItemsets(HashTree tmp){
		System.out.println(tmp.level);
		if(tmp.level == tmp.k+1){
			//chegou na folha
			printItemsets(tmp.itemsets);
		}
		for(HashTree ht: tmp.nodes){
			if(ht != null){
				printAllItemsets(ht);
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
//		hashTree.printAllItemsets(hashTree);
		String[] ts = t.split(" ");
		for(int i = 0; i < ts.length; i++){
			hashTree.find(hashTree, t.split(" "), i, new StringBuilder());
		}
		
	}
}
