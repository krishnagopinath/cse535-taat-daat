import java.util.Comparator;

public class FrequencyComparator implements Comparator<PostingList> {

	@Override
	public int compare(PostingList o1, PostingList o2) {
		// TODO Auto-generated method stub
		if (o1.frequency < o2.frequency) {
			return 1;
		} else if (o1.frequency > o2.frequency) {
			return -1;
		} else {
			return 0;
		}
	}

}
