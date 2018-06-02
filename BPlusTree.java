import java.io.FileNotFoundException;

public class BPlusTree {
	CreateFile root;
	
	public BPlusTree(CreateFile randomAccessFile) throws FileNotFoundException {
		root = randomAccessFile;
	}
	
}