package app;


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
}
