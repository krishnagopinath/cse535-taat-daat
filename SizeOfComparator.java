import java.util.Comparator;


public class SizeOfComparator implements Comparator<PostingListItem>{

	@Override
	public int compare(PostingListItem o1, PostingListItem o2) {
		// TODO Auto-generated method stub
		
		if (o1.sizeOf < o2.sizeOf) {
			return 1;
		} else if (o1.sizeOf > o2.sizeOf) {
			return -1;
		} else {
			return 0;
		}
	}
	
}
