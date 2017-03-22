// Object that will have <Identifier> <Hostname> <Port> read from config file stored
public class Node {
	int node_id;
	String host;
	int port;
	public Node(int node_id, String host, int port) {
		super();
		this.node_id = node_id;
		this.host = host;
		this.port = port;
	}
}