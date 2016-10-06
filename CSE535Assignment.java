import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

public class CSE535Assignment {

	static LinkedList<PostingListItem> postingListItems;
	static String output = null;
	static BufferedWriter writer;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String line = null;
		String term = "term.idx";

		int k = 0;
		String query = null;

		// take args
		if (args.length > 0) {
			term = args[0];
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(args[1]), "utf-8"));
			k = Integer.parseInt(args[2]);
			query = args[3];
		}

		BufferedReader bufferedReader = new BufferedReader(new FileReader(term));

		postingListItems = new LinkedList<PostingListItem>();

		while ((line = bufferedReader.readLine()) != null) {
			// TODO replace \c, \m with ~ when reading from a file.
			// TODO Try merging this with split in the form of a Regex
			String posting = line.replace("\\c", "~").replace("\\m", "~");
			// split by ~
			String[] splitEntry = posting.split("~");

			// make PreIndex structure
			PostingListItem postingListItem = new PostingListItem();

			for (int i = 0; i < splitEntry.length; i++) {
				postingListItem.term = splitEntry[0];
				postingListItem.sizeOf = Integer.parseInt(splitEntry[1]);

				// split by comma
				String[] tempArray = splitEntry[2].split(",");

				// house cleaning
				tempArray[0] = tempArray[0].replace("[", "");
				tempArray[tempArray.length - 1] = tempArray[tempArray.length - 1]
						.replace("]", "");

				LinkedList<PostingList> docIDs = new LinkedList<>();
				LinkedList<PostingList> frequencies = new LinkedList<>();

				// push to postingList
				for (int j = 0; j < tempArray.length; j++) {
					docIDs.add(new PostingList(Integer.parseInt(tempArray[j]
							.split("/")[0].trim()), Integer
							.parseInt(tempArray[j].split("/")[1].trim())));

					frequencies
							.add(new PostingList(Integer.parseInt(tempArray[j]
									.split("/")[0].trim()),
									Integer.parseInt(tempArray[j].split("/")[1]
											.trim())));
				}

				// sort docID
				Collections.sort(docIDs, new DocIDComparator());

				// sort frequency
				Collections.sort(frequencies, new FrequencyComparator());

				// add to PostingListItem
				postingListItem.byDocID = docIDs;
				postingListItem.byFrequencies = frequencies;
			}

			postingListItems.add(postingListItem);

		}

		bufferedReader.close();

		// run topK
		ArrayList<String> topK = getTopK(k);

		WriteToOutputLog(topK, k);

		// read query.txt
		bufferedReader = new BufferedReader(new FileReader(query));

		while ((line = bufferedReader.readLine()) != null) {
			String[] terms = line.split(" ");
			ArrayList<ArrayList<Integer>> result = null;
			for (int i = 0; i < terms.length; i++) {
				result = getPostings(terms[i]);
				WriteToOutputLog(result, terms[i]);
			}

			AtATime TaaTAnd = TermAtATimeQuery(terms, "AND");
			AtATime TaaTOr = TermAtATimeQuery(terms, "OR");
			AtATime DaaTAnd = DocAtATimeQuery(terms, "AND");
			AtATime DaaTOr = DocAtATimeQuery(terms, "OR");

			WriteToOutputLog(
					new AtATime[] { TaaTAnd, TaaTOr, DaaTAnd, DaaTOr }, terms);
		}

		writer.close();

	}

	public static void WriteToOutputLog(ArrayList<String> topK, int k)
			throws UnsupportedEncodingException, FileNotFoundException,
			IOException {

		writer.write("FUNCTION: getTopK " + k);
		writer.newLine();
		writer.write("Result: ");

		writer.write(topK.toString().replaceAll("\\[|\\]", ""));
	}

	public static void WriteToOutputLog(ArrayList<ArrayList<Integer>> postings,
			String query) throws UnsupportedEncodingException,
			FileNotFoundException, IOException {

		/*
		 * FUNCTION: getPostings hard Ordered by doc IDs: 100, 200, 300… Ordered
		 * by TF: 300, 100, 200…
		 */

		writer.newLine();
		writer.write("FUNCTION: getPostings " + query);
		writer.newLine();
		if (postings.get(0).size() != 0) {
			writer.write("Ordered by doc IDs: "
					+ postings.get(0).toString().replaceAll("\\[|\\]", ""));
			writer.newLine();
			writer.write("Ordered by TF: "
					+ postings.get(1).toString().replaceAll("\\[|\\]", ""));
		} else {
			writer.write("term not found");
		}

	}

	public static void WriteToOutputLog(AtATime[] AtATimeResults, String[] terms)
			throws IOException {

		//output log for TAAT and DAAT
		for (int i = 0; i < AtATimeResults.length; i++) {

			writer.newLine();

			writer.write("FUNCTION: " + AtATimeResults[i].TermOrDoc
					+ "AtATimeQuery" + AtATimeResults[i].op + " "
					+ Arrays.toString(terms).replaceAll("\\[|\\]", ""));

			writer.newLine();

			if (AtATimeResults[i].documentsFound != 0) {

				writer.write(AtATimeResults[i].documentsFound
						+ " documents are found");
				writer.newLine();
				writer.write(AtATimeResults[i].comparisonsMade
						+ " comparisions are made");
				writer.newLine();
				// zz seconds are used
				writer.write(AtATimeResults[i].secondsUsed
						+ " seconds are used");
				writer.newLine();

				// nn comparisons are made with optimization (optional bonus
				// part)
				if (AtATimeResults[i].optimizedComparisons != -1) {
					writer.write(AtATimeResults[i].optimizedComparisons
							+ " comparisons are made with optimization (optional bonus part)");
					writer.newLine();
				}

				writer.write("Result: "
						+ AtATimeResults[i].result.toString().replaceAll(
								"\\[|\\]", ""));
			} else {
				writer.write("term not found");
			}

		}

	}

	public static ArrayList<String> getTopK(int k) {
		// init
		LinkedList<PostingListItem> topKSorted = postingListItems;
		ArrayList<String> topKTerms = new ArrayList<>();
		int i = 0;

		// sort
		Collections.sort(topKSorted, new SizeOfComparator());

		// iterate
		ListIterator<PostingListItem> listIterator = topKSorted.listIterator();

		while (listIterator.hasNext() && i < k) {
			topKTerms.add(listIterator.next().term);
			i++;
		}

		return topKTerms;
	}

	public static ArrayList<ArrayList<Integer>> getPostings(String query) {
		// init for docID list
		ArrayList<Integer> byDocIDs = new ArrayList<Integer>();
		// init for frequencies list
		ArrayList<Integer> byFrequencies = new ArrayList<Integer>();

		// find the term
		PostingListItem postingListItem = findPostingListItem(query);
		// get the docIDs
		if (postingListItem != null) {
			ListIterator<PostingList> innerListIterator = postingListItem.byDocID
					.listIterator();

			while (innerListIterator.hasNext()) {
				byDocIDs.add(innerListIterator.next().docID);
			}

			// get the frequencies
			innerListIterator = postingListItem.byFrequencies.listIterator();

			while (innerListIterator.hasNext()) {
				byFrequencies.add(innerListIterator.next().docID);
			}

		}

		// return result
		ArrayList<ArrayList<Integer>> getPostingsResult = new ArrayList<ArrayList<Integer>>();

		getPostingsResult.add(byDocIDs);
		getPostingsResult.add(byFrequencies);

		return getPostingsResult;
	}

	public static AtATime TermAtATimeQuery(String[] queries, String op) {

		AtATime result = new AtATime();
		switch (op.toUpperCase()) {
		case "AND":
			result = TaaTAnd(queries);
			break;
		case "OR":
			result = TaaTOr(queries);
			break;
		}

		return result;
	}

	public static AtATime DocAtATimeQuery(String[] queries, String op) {

		AtATime result = new AtATime();
		switch (op.toUpperCase()) {
		case "AND":
			result = DaaTAnd(queries);
			break;
		case "OR":
			result = DaaTOr(queries);
			break;
		}

		return result;
	}

	static AtATime TaaTAnd(String[] queries) {

		// init criteria variables
		int comparisons = 0;

		// init timer
		long startTime = System.currentTimeMillis();

		// init docIDStash aka stash
		LinkedList<Integer> docIDStash = new LinkedList<Integer>();

		// exit flag
		boolean shouldIStop = false;

		// list for optimized TaaT AND
		ArrayList<LinkedList<Integer>> preOptimizedList = new ArrayList<LinkedList<Integer>>();

		// start timer
		/* Non optimized */
		// loop queries
		for (String query : queries) {
			if (!shouldIStop) {
				// find the term
				PostingListItem postingListItem = findPostingListItem(query);
				LinkedList<Integer> currentTermDocIDs = new LinkedList<Integer>();

				if (postingListItem != null) {

					// make docID linkedList from the frequency sorted list
					ListIterator<PostingList> innerListIterator = postingListItem.byFrequencies
							.listIterator();

					while (innerListIterator.hasNext()) {
						currentTermDocIDs.add(innerListIterator.next().docID);
					}

					preOptimizedList.add(currentTermDocIDs);

					// check if stash is empty
					if (docIDStash.size() == 0) {
						// add to stash
						docIDStash = currentTermDocIDs;
					} else {
						// else, run intersection
						/*
						 * START INTERSECT
						 */

						// create intersection linked list
						LinkedList<Integer> intersectionDocIDs = new LinkedList<Integer>();

						// set up iterators
						ListIterator<Integer> stashIterator = docIDStash
								.listIterator();

						// loop with exit criterion while
						while (stashIterator.hasNext()) {
							Integer stashID = stashIterator.next();
							ListIterator<Integer> currentIterator = currentTermDocIDs
									.listIterator();

							while (currentIterator.hasNext()) {
								comparisons += 1;
								if (stashID.equals(currentIterator.next())) {
									intersectionDocIDs.add(stashID);
								}
							}
						}

						// if no intersection is found, flag the EXIT
						if (intersectionDocIDs.size() == 0) {
							shouldIStop = true;
							// clear docIDStash
							docIDStash = new LinkedList<Integer>();
						} else {
							// if intersection(s) are found, update docIDStash
							docIDStash = intersectionDocIDs;
						}
						// keep going till queries are done

						/*
						 * END INTERSECT
						 */
					}
				} else {
					shouldIStop = true;
					docIDStash = new LinkedList<>();
				}
			}
		}

		// end timer
		long endTime = System.currentTimeMillis();

		ArrayList<Integer> result = new ArrayList<Integer>(docIDStash);
		Collections.sort(result);

		// construct AtATime structure (for output)
		AtATime termAtATimeAnd = new AtATime();
		termAtATimeAnd.TermOrDoc = "term";
		termAtATimeAnd.op = "And";
		termAtATimeAnd.secondsUsed = (float) (endTime - startTime) / 1000;
		termAtATimeAnd.comparisonsMade = comparisons;
		termAtATimeAnd.documentsFound = docIDStash.size();
		termAtATimeAnd.result = result;

		// optimized version
		// sort
		Collections.sort(preOptimizedList,
				new Comparator<LinkedList<Integer>>() {

					@Override
					public int compare(LinkedList<Integer> o1,
							LinkedList<Integer> o2) {
						if (o1.size() > o2.size()) {
							return 1;
						} else if (o1.size() < o2.size()) {
							return -1;
						} else {
							return 0;
						}
					}
				});

		comparisons = 0;
		docIDStash = new LinkedList<Integer>();
		shouldIStop = false;

		// repeat
		for (LinkedList<Integer> currentTermDocIDs : preOptimizedList) {

			if (!shouldIStop) {

				// check if stash is empty
				if (docIDStash.size() == 0) {
					// add to stash
					docIDStash = currentTermDocIDs;
				} else {
					// else, run intersection
					/*
					 * START INTERSECT
					 */

					// create intersection linked list
					LinkedList<Integer> intersectionDocIDs = new LinkedList<Integer>();

					// set up iterators
					ListIterator<Integer> stashIterator = docIDStash
							.listIterator();

					// loop with exit criterion while
					while (stashIterator.hasNext()) {
						Integer stashID = stashIterator.next();
						ListIterator<Integer> currentIterator = currentTermDocIDs
								.listIterator();

						while (currentIterator.hasNext()) {
							comparisons += 1;
							if (stashID.equals(currentIterator.next())) {
								intersectionDocIDs.add(stashID);
							}
						}
					}

					// if no intersection is found, flag the EXIT
					if (intersectionDocIDs.size() == 0) {
						shouldIStop = true;
						// clear docIDStash
						docIDStash = new LinkedList<Integer>();
					} else {
						// if intersection(s) are found, update docIDStash
						docIDStash = intersectionDocIDs;
					}
					// keep going till queries are done

					/*
					 * END INTERSECT
					 */
				}
			} else {
				shouldIStop = true;
				docIDStash = new LinkedList<>();
			}
		}

		termAtATimeAnd.optimizedComparisons = comparisons;
		return termAtATimeAnd;
	}

	static AtATime TaaTOr(String[] queries) {

		// init docIDStash aka stash
		LinkedList<Integer> docIDStash = new LinkedList<Integer>();

		int comparisons = 0;

		// list for optimized TaaT OR
		ArrayList<LinkedList<Integer>> preOptimizedList = new ArrayList<LinkedList<Integer>>();

		// start timer
		/* Non optimized */

		// init timer
		long startTime = System.currentTimeMillis();

		// loop through queries
		for (String query : queries) {
			// find term
			PostingListItem postingListItem = findPostingListItem(query);
			// get docIDs for term and add to stash
			LinkedList<Integer> currentTermDocIDs = new LinkedList<Integer>();

			if (postingListItem != null) {
				// make docID linkedList from the frequency sorted list
				ListIterator<PostingList> innerListIterator = postingListItem.byFrequencies
						.listIterator();

				while (innerListIterator.hasNext()) {
					currentTermDocIDs.add(innerListIterator.next().docID);
				}

			}

			preOptimizedList.add(currentTermDocIDs);

			// check if stash is empty
			if (docIDStash.size() == 0) {
				// add to stash
				docIDStash = currentTermDocIDs;
			} else {
				// else, perform UNION

				// maintain a remainder list
				LinkedList<Integer> remainderList = new LinkedList<Integer>();

				for (int i = 0; i < currentTermDocIDs.size(); i++) {
					boolean currentIDExists = false;
					int currentID = currentTermDocIDs.get(i);

					for (int j = 0; j < docIDStash.size(); j++) {
						// check if currentID exists in stashList

						if (currentID == docIDStash.get(j)) {
							comparisons++;
							currentIDExists = true;
							break;
						} else {
							comparisons++;
						}
					}

					if (!currentIDExists) {
						remainderList.add(currentID);
					}

				}

				// move unionIntermediate to stash
				docIDStash.addAll(remainderList);
			}

		}

		// end timer
		long endTime = System.currentTimeMillis();

		ArrayList<Integer> result = new ArrayList<Integer>(docIDStash);
		Collections.sort(result);

		// construct AtATime structure (for output)
		AtATime termAtATimeOr = new AtATime();
		termAtATimeOr.TermOrDoc = "term";
		termAtATimeOr.op = "Or";
		termAtATimeOr.secondsUsed = (float) (endTime - startTime) / 1000;
		termAtATimeOr.comparisonsMade = comparisons;
		termAtATimeOr.documentsFound = docIDStash.size();
		termAtATimeOr.result = result;

		// optimized version
		// sort
		Collections.sort(preOptimizedList,
				new Comparator<LinkedList<Integer>>() {

					@Override
					public int compare(LinkedList<Integer> o1,
							LinkedList<Integer> o2) {
						if (o1.size() > o2.size()) {
							return 1;
						} else if (o1.size() < o2.size()) {
							return -1;
						} else {
							return 0;
						}
					}
				});

		comparisons = 0;
		docIDStash = new LinkedList<Integer>();

		// repeat
		for (LinkedList<Integer> currentTermDocIDs : preOptimizedList) {

			// check if stash is empty
			if (docIDStash.size() == 0) {
				// add to stash
				docIDStash = currentTermDocIDs;
			} else {
				// else, perform UNION

				// maintain a remainder list
				LinkedList<Integer> remainderList = new LinkedList<Integer>();

				for (int i = 0; i < currentTermDocIDs.size(); i++) {
					boolean currentIDExists = false;
					int currentID = currentTermDocIDs.get(i);

					for (int j = 0; j < docIDStash.size(); j++) {
						// check if currentID exists in stashList

						if (currentID == docIDStash.get(j)) {
							comparisons++;
							currentIDExists = true;
							break;

						} else {
							comparisons++;
						}
					}

					if (!currentIDExists) {
						remainderList.add(currentID);
					}

				}

				// move unionIntermediate to stash
				docIDStash.addAll(remainderList);
			}
		}

		termAtATimeOr.optimizedComparisons = comparisons;

		return termAtATimeOr;

	}

	@SuppressWarnings("rawtypes")
	static AtATime DaaTAnd(String[] queries) {

		int comparisons = 0;
		boolean weGotNull = false;

		long startTime = System.currentTimeMillis();
		/* create empty intersections list */
		LinkedList<Integer> intersections = new LinkedList<Integer>();

		/*
		 * read all terms and get their corresponding posting lists as
		 * posting_list[]
		 */
		// create posting_list[]
		List<Integer>[] postingLists = new LinkedList[queries.length];

		// loop over queries
		for (int i = 0; i < postingLists.length; i++) {
			// get postingResult and get the list sorted by docIDs
			PostingListItem postingListItem = findPostingListItem(queries[i]);

			LinkedList<Integer> currentTermDocIDs = new LinkedList<Integer>();

			if (postingListItem != null) {
				// make docID linkedList from the frequency sorted list
				ListIterator<PostingList> innerListIterator = postingListItem.byDocID
						.listIterator();

				while (innerListIterator.hasNext()) {
					currentTermDocIDs.add(innerListIterator.next().docID);
				}

				postingLists[i] = currentTermDocIDs;
			} else {
				weGotNull = true;
				break;
			}

		}

		if (!weGotNull) {
			/*
			 * Follows a revolving door approach. An array holds the docID
			 * values in each list. No of terms == size of the array.
			 * As the pointers are moved, the item in that array are changed.
			 */
			int size = postingLists.length;
			Iterator[] it = new Iterator[size];
			for (int i = 0; i < size; i++) {
				it[i] = postingLists[i].iterator();
			}

			Integer[] currentTerms = new Integer[size];
			for (int k = 0; k < size; k++) {
				currentTerms[k] = (Integer) it[k].next();
			}
			Integer max = currentTerms[0];
			while (true) {
				int countOfEqualComparisons = 0;
				for (int x = 0; x < size; x++) {
					if (max.compareTo(currentTerms[x]) < 0) {
						comparisons++;
						max = currentTerms[x];
					} else if (max.compareTo(currentTerms[x]) == 0) {
						countOfEqualComparisons++;
						comparisons++;
					}
				}
				int flag = 1;
				if (countOfEqualComparisons == size) {
					intersections.add(max);
					for (int j = 0; j < size; j++) {
						if (!it[j].hasNext()) {
							flag = 0;
							break;
						} else {
							currentTerms[j] = (Integer) it[j].next();
						}

					}

				} else {
					for (int j = 0; j < size; j++) {

						if (max.compareTo(currentTerms[j]) != 0) {
							if (it[j].hasNext()) {
								currentTerms[j] = (Integer) it[j].next();
							} else {
								flag = 0;
								break;
							}

						}

					}
				}

				if (flag == 0) {
					break;
				}

			}

		}

		long endTime = System.currentTimeMillis();

		ArrayList<Integer> result = new ArrayList<>(intersections);

		Collections.sort(result);

		// construct AtATime structure (for output)
		AtATime termAtATimeAnd = new AtATime();
		termAtATimeAnd.TermOrDoc = "doc";
		termAtATimeAnd.op = "And";
		termAtATimeAnd.secondsUsed = (float) (endTime - startTime) / 1000;
		termAtATimeAnd.comparisonsMade = comparisons;
		termAtATimeAnd.documentsFound = intersections.size();
		termAtATimeAnd.result = result;

		return termAtATimeAnd;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static AtATime DaaTOr(String[] queries) {
		/*
		 * read all terms and get their corresponding posting lists as
		 * posting_list[]
		 */
		// create posting_list[]

		int comparisons = 0;

		long startTime = System.currentTimeMillis();

		List<Integer>[] postingLists = new LinkedList[queries.length];

		// loop over queries
		for (int i = 0; i < postingLists.length; i++) {
			// get postingResult and get the list sorted by docIDs
			PostingListItem postingListItem = findPostingListItem(queries[i]);

			LinkedList<Integer> currentTermDocIDs = new LinkedList<Integer>();

			if (postingListItem != null) {
				// make docID linkedList from the frequency sorted list
				ListIterator<PostingList> innerListIterator = postingListItem.byDocID
						.listIterator();

				while (innerListIterator.hasNext()) {
					currentTermDocIDs.add(innerListIterator.next().docID);
				}
			}

			postingLists[i] = currentTermDocIDs;

		}

		/*
		 * Follows a revolving door approach. An array holds the docID
		 * values in each list. No of terms == size of the array.
		 * As the pointers are moved, the item in that array are changed.
		 */
		int size = postingLists.length;
		Iterator[] it = new Iterator[size];
		for (int i = 0; i < size; i++) {
			it[i] = postingLists[i].iterator();
		}
		List<Integer> union = new LinkedList<>();

		while (true) {
			for (int i = 0; i < size; i++) {
				Integer integ;
				if (it[i].hasNext()) {
					integ = (Integer) it[i].next();
					int flag = 1;
					for (Iterator iter = union.iterator(); iter.hasNext();) {
						if (integ.equals(iter.next())) {
							comparisons++;
							flag = 0;
							break;
						} else {
							comparisons++;
						}
					}

					if (flag != 0) {
						union.add(integ);
					}
				}
				int canContinue = 0;
				for (int k = 0; k < size; k++) {
					if (it[k].hasNext()) {
						canContinue = 1;
						break;
					}
				}
				if (canContinue == 0) {

					long endTime = System.currentTimeMillis();

					ArrayList<Integer> result = new ArrayList<Integer>(union);
					Collections.sort(result);

					// construct AtATime structure (for output)
					AtATime termAtATimeOr = new AtATime();

					termAtATimeOr.TermOrDoc = "doc";
					termAtATimeOr.op = "Or";
					termAtATimeOr.secondsUsed = (float) (endTime - startTime) / 1000;
					termAtATimeOr.comparisonsMade = comparisons;
					termAtATimeOr.documentsFound = union.size();
					termAtATimeOr.result = result;

					return termAtATimeOr;

				}

			}
		}

	}

	//gets the posting list based on term 
	static PostingListItem findPostingListItem(String term) {
		ListIterator<PostingListItem> listIterator = postingListItems
				.listIterator();

		PostingListItem postingListResult = null;

		// find the term
		while (listIterator.hasNext() && postingListResult == null) {
			PostingListItem postingListItem = listIterator.next();
			if (postingListItem.term.equals(term)) {
				// returning here seems to mess up GC
				postingListResult = postingListItem;
			}
		}
		return postingListResult;
	}

}
