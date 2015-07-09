package main.java.com.mestrado.app;


public class HashPrefixTree {
	private HashNode hashNode;
	
	public HashPrefixTree() {
		this.hashNode = new HashNode(0);
	}
	
	public HashNode getHashNode() {
		return hashNode;
	}

	public void setHashNode(HashNode hashNode) {
		this.hashNode = hashNode;
	}

	/**
	 * 
	 * @param hNode
	 * @param itemset
	 * @param i
	 */
	public void add(HashNode hNode, String[] itemset, int i){
		if(i >= itemset.length){
			return;
		}
		
		HashNode son = hNode.getHashNode().get(itemset[i]);
		
		if(son == null){
			//Cria um novo registro na hash
			hNode.getHashNode().put(itemset[i], new HashNode(i+1));
			if(i >= itemset.length-1){
				return;
			}
			add(hNode.getHashNode().get(itemset[i]), itemset, i+1);
		}else{
			//percorre a hashTree
			i++;
			add(son,itemset,i);
		}
	}
	
	/**
	 * 
	 * @param transaction
	 * @param hNode
	 * @param i
	 * @param k
	 * @param itemset
	 * @param itemIndex
	 */
	public void findWithOutK(String[] transaction, HashNode hNode, int i, int k, String[] itemset, int itemIndex){
		if(i >= transaction.length){
			return;
		}
		
		HashNode son = hNode.getHashNode().get(transaction[i]);
		
		if(son == null){
			return;
		}else{
			itemset[itemIndex] = transaction[i];
			
			StringBuilder sb = new StringBuilder();
			for(String item: itemset){
				if(item != null){
					sb.append(item).append(" ");
				}
			}
			
			System.out.println("Encontrou: "+sb.toString().trim());
			i++;
			itemIndex++;
			while(i < transaction.length){
				findWithOutK(transaction, son, i, k, itemset, itemIndex);
				for(int j = itemIndex; j < itemset.length; j++){
					itemset[j] = "";
				}
				i++;
			}
		}
	}
	
	public void findWithK(String[] transaction, HashNode hNode, int i, int k, String[] itemset, int itemIndex){
		if(i >= transaction.length){
			return;
		}
		
		HashNode son = hNode.getHashNode().get(transaction[i]);
		
		if(son == null){
			return;
		}else{
			itemset[itemIndex] = transaction[i];
			if(hNode.getLevel() == k-1){
				StringBuilder sb = new StringBuilder();
				for(String item: itemset){
					if(item != null){
						sb.append(item).append(" ");
					}
				}
				System.out.println("Encontrou: "+sb.toString().trim());
				itemset[itemIndex] = "";
				return;
			}
			
			i++;
			itemIndex++;
			while(i < transaction.length){
				findWithK(transaction, son, i, k, itemset, itemIndex);
				for(int j = itemIndex; j < itemset.length; j++){
					itemset[j] = "";
				}
				i++;
			}
		}
	}
	
	public static void main(String[] args) {
		HashPrefixTree hpt = new HashPrefixTree();
		String[] itemset = {"a","b","c","d"};
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[1] = "c";
		itemset[2] = "d";
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[0] = "b";
		itemset[1] = "c";
		itemset[2] = "d";
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[0] = "b";
		itemset[1] = "c";
		itemset[2] = "e";
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[1] = "c";
		itemset[2] = "h";
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[0] = "c";
		itemset[1] = "d";
		itemset[2] = "e";
		hpt.add(hpt.getHashNode(), itemset, 0);
		System.out.println("Inserido\nBucando...");
		String[] transaction = {"a","x","b","j","c","d","h","e"};
		for(int i = 0; i < transaction.length; i++){
			itemset = new String[4];
			hpt.findWithK(transaction, hpt.getHashNode(), i, 4, itemset, 0);
		}
	}
}