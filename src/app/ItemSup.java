package app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @author eduardo
 *
 */
public class ItemSup {
	private Text itemset;
	private IntWritable support;
	
	public ItemSup(){
		itemset = new Text();
		support = new IntWritable();
	}
	
	public ItemSup(Text itemset, IntWritable support) {
		super();
		this.itemset = itemset;
		this.support = support;
	}
	public Text getItemset() {
		return itemset;
	}
	public void setItemset(Text itemset) {
		this.itemset = itemset;
	}
	public IntWritable getSupport() {
		return support;
	}
	public void setSupport(IntWritable support) {
		this.support = support;
	}
}
