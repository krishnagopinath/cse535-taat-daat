import java.util.LinkedList;


public class PostingListItem {
	public String term;		
	public int sizeOf;
	LinkedList<PostingList> byDocID = new LinkedList<PostingList>();
	LinkedList<PostingList> byFrequencies = new LinkedList<PostingList>();
}
