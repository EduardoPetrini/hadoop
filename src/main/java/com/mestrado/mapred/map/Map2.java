/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import main.java.com.mestrado.app.PrefixTree;
import main.java.com.mestrado.utils.MrUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Gerar itemsets de tamanho 2.
 * 
 * @author eduardo
 */
public class Map2 extends Mapper<LongWritable, Text, Text, Text> {

	private Log log = LogFactory.getLog(Map2.class);
	private SequenceFile.Reader reader;
	private ArrayList<String> blocksIds;
	private String splitName;
	private String inputPartialName;
	private HashMap<String, Integer> itemSup;
	private PrefixTree prefixTree;
	private int maxK;
	/**
	 * Le o arquivo invertido para a memória.
	 * 
	 * @param context
	 * @throws IOException
	 */
	@Override
	public void setup(Context context) throws IOException {
		log.info("Iniciando Map 2");
		int totalPartitions = Integer.parseInt(context.getConfiguration().get(
				"totalPartitions"));
		inputPartialName = context.getConfiguration().get("outputPartialName");
		String count = context.getConfiguration().get("count");

		blocksIds = new ArrayList<String>();
		for (int i = 1; i <= totalPartitions; i++) {
			blocksIds.add(context.getConfiguration().get("blockId" + i)
					.replace("partition", ""));// Id da partição é o offset do
												// bloco
		}
	}

	public boolean checkPartition() {

		for (String ids : blocksIds) {
			System.out
					.println("Verificando se a partição atual será processada: "
							+ splitName + " == " + ids);
			if (MrUtils.checkPartitions(ids, splitName, 100)) {
				splitName = ids;
				return true;
			}
		}

		return false;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		// key é o offset, id do bloco/partição
		splitName = String.valueOf(key.get());
		System.out.println("Id da partição: " + splitName);
		// Verificar se é uma partição a ser processada

		if (checkPartition()) {
			// A partição atual será processada
			System.out
					.println("A partição atual será processada: " + splitName);
			itemSup = new HashMap<String, Integer>();
			prefixTree = new PrefixTree(0);
			openFile(context);// Ler o arquivo da partição para a prefixTree

			String[] itemset;
			String[] tr;
			int itemsetIndex;
			boolean endBlock = false;
			int pos;
			int start = 0;
			int len;

			while ((pos = value.find("\n", start)) != -1) {
				len = pos - start;
				try {
					tr = Text.decode(value.getBytes(), start, len).split(" ");
					for (int i = 0; i < tr.length; i++) {
						itemset = new String[maxK];
						itemsetIndex = 0;
						subSet(tr, prefixTree, i, itemset, itemsetIndex);
					}
				} catch (CharacterCodingException e) {
					e.printStackTrace();
					System.exit(1);
				}
				start = pos + 1;
				if (start >= value.getLength()) {
					System.out.println("Break... " + value.getLength());
					endBlock = true;
					break;
				}
			}
			
			// pegar a ultima transação, caso tenha
			if (!endBlock) {
				len = value.getLength() - start;
				try {
					tr = Text.decode(value.getBytes(), start, len).split(" ");
					for (int i = 0; i < tr.length; i++) {
						itemset = new String[maxK];
						itemsetIndex = 0;
						subSet(tr, prefixTree, i, itemset, itemsetIndex);
					}
				} catch (CharacterCodingException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
			sendItemsetsToReduce(context); 
			// for(ItemSup item: lPartialItemsets){
			// supportLocal = count(transacions, item.getItemset().toString());
			//
			// if(supportLocal > 0){
			// try{
			// valueOut.set(String.valueOf(item.getSupport())+"#"+supportLocal);
			// keyOut.set(item.getItemset());
			//
			// context.write(keyOut, valueOut);
			// }catch(IOException | InterruptedException e){
			// e.printStackTrace();
			// }
			// }
			// }
		}
	}

	private void sendItemsetsToReduce(Context context) {
		Set<String> keys = itemSup.keySet();
		Text valueOut = new Text();
		Text keyOut = new Text();
		
		for(String k: keys){
			keyOut.set(k);
			valueOut.set(String.valueOf(itemSup.get(k)));
			try {
				context.write(keyOut, valueOut);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * 
	 * @param transactions
	 * @param itemset
	 * @return
	 */
	public int count(String[] transactions, String itemset) {
		int count;
		int occurrenceCount = 0;
		int i, j;
		String[] tSplit;
		String[] itemsetSplit = itemset.split(" ");
		boolean checkOccurrence = true;
		for_trans: for (String transaction : transactions) {
			i = 0;
			j = 0;
			count = 0;
			tSplit = transaction.split(" ");

			if (tSplit.length >= itemsetSplit.length) {
				while (checkOccurrence) {

					try {
						if (itemsetSplit[i].equals(tSplit[j])) {
							count++;
							if (++i >= itemsetSplit.length
									|| ++j >= tSplit.length) {
								break;
							}

						} else {
							if (++j >= tSplit.length) {
								continue for_trans;
							}
						}
					} catch (Exception e) {
						System.out.println(Arrays.asList(itemsetSplit) + " == "
								+ Arrays.asList(tSplit));
						System.out.println(i + " " + j);
						e.printStackTrace();
						System.exit(0);
					}
				}
				if (count == itemsetSplit.length) {
					occurrenceCount++;
				}
			}
		}

		return occurrenceCount;
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
			String[] itemset, int itemsetIndex) {
		if (i >= transaction.length) {
			return;
		}
		int index = -1;

		try {
			index = pt.getPrefix().indexOf(transaction[i]);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Prefix: " + pt.getPrefix() + " item: "
					+ transaction[i]);
			System.out.println("Error");
		}

		if (index == -1) {
			return;
		} else {
			itemset[itemsetIndex] = transaction[i];
			i++;
			
			StringBuilder sb = new StringBuilder();
			// System.out.println("Achou2 "+Arrays.asList(itemset));
			for (String s : itemset) {
				if (s != null)
					sb.append(s).append(" ");
			}
			addToHashItemSup(sb.toString().trim());
//			itemset[itemsetIndex] = "";
		
			if (pt.getPrefixTree().isEmpty()
					|| pt.getPrefixTree().size() <= index
					|| pt.getPrefixTree().get(index) == null) {
				itemset[itemsetIndex] = "";
				return;
			} else {
				itemsetIndex++;
				while (i < transaction.length) {
					subSet(transaction, pt.getPrefixTree().get(index), i, itemset, itemsetIndex);
					i++;
				}
			}

		}
	}

	/**
	 * 
	 * @param context
	 */
	public void openFile(Context context) {
		Path path = new Path(inputPartialName + splitName);

		try {
			System.out.println("Lendo a partição '" + path.getName()
					+ "' para a memória!");
			reader = new SequenceFile.Reader(context.getConfiguration(),
					SequenceFile.Reader.file(path));

			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
					context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), context.getConfiguration());
			String k;
			String[] kSpt;
			maxK = 0;
			while (reader.next(key, value)) {
				// System.out.println("Add Key: "+key.toString());
				k = key.toString();
				kSpt = k.split(" ");
				itemSup.put(k, value.get());
				prefixTree.add(prefixTree, kSpt, 0);
				if(kSpt.length > maxK){
					maxK = kSpt.length; 
				}
			}

		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	public boolean addToHashItemSup(String item) {
		Integer value = 0;

		if ((value = itemSup.get(item)) == null) {
			itemSup.put(item, 1);
			return true;
		} else {
			value++;
			itemSup.put(item, value);
			return false;
		}
	}
}
