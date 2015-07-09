package main.java.com.mestrado.app;


/**
 * @author eduardo
 *
 */
public class ItemSup {
	private String itemset;
	private int support;
	
	public ItemSup(){
		itemset = new String();
		support = 0;
	}
	
	public ItemSup(String itemset) {
		super();
		this.itemset = itemset;
		this.support = 1;
	}

	public ItemSup(String itemset, int support) {
		super();
		this.itemset = itemset;
		this.support = support;
	}
	public String getItemset() {
		return itemset;
	}
	public void setItemset(String itemset) {
		this.itemset = itemset;
	}
	public int getSupport() {
		return support;
	}
	public void setSupport(int support) {
		this.support = support;
	}

	@Override
	public boolean equals(Object obj) {
		ItemSup newItem = (ItemSup)obj;
		
		if(this.itemset.equalsIgnoreCase(newItem.itemset)) return true;
		return false;
	}

	@Override
	public String toString() {
		return itemset+":"+support;
	}
	
	public void increSupport(){
		this.support++;
	}
}
