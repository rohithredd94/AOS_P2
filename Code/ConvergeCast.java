import java.util.LinkedList;
import java.util.Queue;

//QNode stores the node value and level
class QNode{
	int node;
	int level;
	
	public QNode(int i, int j) {
		this.node = i;
		this.level = j;
	}
}
public class ConvergeCast {
	
	static int[] parent;
	
	//Function that returns parent
	public static int getParent(int id) {
		return parent[id];
	}
	
	static //Function that implements bfs to build spanning tree
	

//	public static void main(String[] args){
//		int[][] adjMatrix ={ { 0,0,0,0,1},{1,0,0,0,0},{0,0,0,1,0},{0,1,0,0,0},{0,0,1,0,0}};
//		build_tree(adjMatrix);
//		for(int i=0;i<adjMatrix.length;i++)
//		System.out.println("Node  "+i+" Parent is "+getParent(i));
//	}
}