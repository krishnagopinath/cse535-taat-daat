import java.util.Comparator;

public class DocIDComparator implements Comparator<PostingList> {

	@Override
	public int compare(PostingList o1, PostingList o2) {
		// TODO Auto-generated method stub
		if (o1.docID > o2.docID) {
			return 1;
		} else if (o1.docID < o2.docID) {
			return -1;
		} else {
			return 0;
		}
	}
}
