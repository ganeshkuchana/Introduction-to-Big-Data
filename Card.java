import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Card implements WritableComparable<Card> {
    private String suit;
    private String rank;
    
    public Card() {}
    
    public Card(String suit, String rank) {
        this.suit = suit;
        this.rank = rank;
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeUTF(suit);
        out.writeUTF(rank);
    }
    
    public void readFields(DataInput in) throws IOException {
        suit = in.readUTF();
        rank = in.readUTF();
    }
    
    public int compareTo(Card other) {
        int suitCompare = this.suit.compareTo(other.suit);
        if (suitCompare != 0) return suitCompare;
        return this.rank.compareTo(other.rank);
    }
    
    @Override
    public String toString() {
        return rank + " of " + suit;
    }
}
