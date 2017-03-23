import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.LinkedList;
import java.util.Queue;

@SuppressWarnings("serial")
public class NodeServer implements Serializable  {
	static String output_file_name;
	int id;
	int num_of_nodes, min_per_active, max_per_active, min_send_delay, snapshot_delay, max_number;
	int total_messages_sent = 0;
	boolean active=false;
	int[][] adj_matrix;
	int[] current_time_stamp;
	int[] neighbors;
	boolean is_blue = true;
	int logging = 0;
	boolean is_first_time = true;
	int parent;
	//ArrayLintst which holds the nodes part of the distributed system 
	ArrayList<Node> nodes = new ArrayList<Node>();
	//HashMap which has node number as keys and <id,host,port> as value
	//Create all the channels in the beginning and keep it open till the end
	HashMap<Integer,Socket> channels = new HashMap<Integer,Socket>();
	//Create all the output streams associated with each socket 
	HashMap<Integer,ObjectOutputStream> output_stream = new HashMap<Integer,ObjectOutputStream>();
	//HashMap which stores ArrayList of messages recorded while the process is red for each channel
	HashMap<Integer,ArrayList<ApplicationMsg>> in_transit_msgs;
	//HashMap which stores all incoming channels and boolean received marker message
	HashMap<Integer,Boolean> rec_marker;
	//HashMap which stores all state messages
	HashMap<Integer,StateMsg> state_messages;	
	//Used to determine if state message has been received from all the processes in the system
	boolean[] allnodes_state_msg;
	//Every process stores its state(current_time_stamp,in_transit_msgs and its id) in this StateMsg Object
	StateMsg my_state;
	//To hold output Snapshots
	ArrayList<int[]> output = new ArrayList<int[]>();

	//Re-initialize everything that is needed for Chandy Lamport protocol before restarting it
	public void initialize() {
		this.in_transit_msgs = new HashMap<Integer,ArrayList<ApplicationMsg>>();
		this.rec_marker = new HashMap<Integer,Boolean>();
		this.state_messages = new HashMap<Integer,StateMsg>();	

		//Initialize in_transit_msgs hashMap
		for(Integer element : this.channels.keySet())
			this.in_transit_msgs.put(element, new ArrayList<ApplicationMsg>());
		//Initialize boolean hashmap rec_marker to false
		for(Integer e: this.neighbors)
			this.rec_marker.put(e,false);
		this.allnodes_state_msg = new boolean[this.num_of_nodes];
		this.my_state = new StateMsg();
		this.my_state.current_time_stamp = new int[this.num_of_nodes];
	}

	public void buildTree(int[][] matrix) {
		boolean[] visited = new boolean[matrix.length];
		Queue<QNode> queue = new LinkedList<QNode>();
		queue.add(new QNode(0,0));
		//If its already visited then no need to visit again since its done in bfs tree , nodes visited at first level will have direct parents and so on
		visited[0] = true;
		while(!queue.isEmpty()){
			QNode u = queue.remove();
			for(int i=0;i<matrix[u.node].length;i++){
				if(matrix[u.node][i] == 1 && visited[i] == false){
					queue.add(new QNode(i,u.level+1));
					if(this.id == i)
						this.parent = u.node;
					visited[i] = true;
				}
			}
		}
	}

	class QNode{
		int node;
		int level;
		
		public QNode(int i, int j) {
			this.node = i;
			this.level = j;
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		//Read the values for all variables from the configuration file
		NodeServer obj_main = ConfigParser.readConfigFile(args[1]);
		// Get the node number of the current Node
		obj_main.id = Integer.parseInt(args[0]);
		int current_node = obj_main.id;
		//Get the configuration file from command line
		NodeServer.output_file_name = args[1].substring(0, args[1].lastIndexOf('.'));
		//Build converge cast spanning tree in the beginning
		obj_main.buildTree(obj_main.adj_matrix);
		//Transfer the collection of nodes to hash map which has node id as key since we need to get and node as value ,it returns <id,host,port> when queried with node Id.
		HashMap<Integer, Node> node_address_list = new HashMap<Integer, Node>();
		for(int i=0;i<obj_main.nodes.size();i++){
			node_address_list.put(obj_main.nodes.get(i).node_id, obj_main.nodes.get(i));
		}
		//Get the port number on which this node should listen 
		int server_port = obj_main.nodes.get(obj_main.id).port;
		//Start server on this node's assigned port
		ServerSocket server_listener = new ServerSocket(server_port);
		Thread.sleep(10000);
		//Create channels and keep it till the end
		for(int i=0;i<obj_main.num_of_nodes;i++){
			// If the value in adjacency matrix is one for the current Node then its a neighbor
			if(obj_main.adj_matrix[current_node][i] == 1){
				//InetAddress address = InetAddress.getByName(hostName);
				Socket client = new Socket(node_address_list.get(i).host, node_address_list.get(i).port);
				obj_main.channels.put(i, client);
				// Get an output stream associated with each socket and put it in a hashmap output_stream
				ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
				obj_main.output_stream.put(i, oos);
			}
		}

		//Populate neighbors array 
		Set<Integer> keys = obj_main.channels.keySet();
		obj_main.neighbors = new int[keys.size()];
		int index = 0;
		for(Integer element : keys)
			obj_main.neighbors[index++] = element.intValue();
		//obj_main.current_time_stamp is used to maintain the current timestamp of the process
		obj_main.current_time_stamp = new int[obj_main.num_of_nodes];

		//Initialize all the datastructures needed for the node to run the protocols
		obj_main.initialize();

		//Initially node 0 is active therefore if this node is 0 then it should be active
		if(current_node == 0){
			obj_main.active = true;
			System.out.println("Emitted Messages");			
			//Start Chandy Lamport protocol if it is node 0
			new CLThread(obj_main).start();
			new MAPThread(obj_main).start();
		}
		try {
			while (true) {
				// This node listens as a Server for the clients requests 
				Socket socket = server_listener.accept();
				// For every client request start a new thread 
				new ClientThread(socket,obj_main).start();
			}
		}
		finally {
			server_listener.close();
		}
	}


	public void startMAPProtocol() throws InterruptedException {

		//Get a random number between min_per_active to max_per_active to emit that many messages
		int num_of_msgs = 1;
		num_of_msgs = this.getRandomNumber(this.min_per_active,this.max_per_active);
		//If random number is 0 then since node 0 is the only process active in the beginning it will not start therefore get a bigger random number
		if(num_of_msgs == 0)
			num_of_msgs = this.getRandomNumber(this.min_per_active + 1,this.max_per_active);
		//Channels hashMap has all neighbors as keys, store them in an array to get random neighbor
		for(int i=0;i<num_of_msgs;i++) {
			synchronized(this) {
				//Get a random number to index in the neighbors and array and get that neighbor
				int neighbor_index = this.getRandomNumber(0,this.neighbors.length-1);
				int current_neighbor = this.neighbors[neighbor_index];
				if(this.active == true){
					//send application message
					ApplicationMsg app_msg = new ApplicationMsg(); 
					// Code for current_time_stamp protocol
					this.current_time_stamp[this.id]++;
					app_msg.current_time_stamp = new int[this.current_time_stamp.length];
					System.arraycopy(this.current_time_stamp, 0, app_msg.current_time_stamp, 0, this.current_time_stamp.length);
					app_msg.node_id = this.id;
					// Write the message in the channel connecting to neighbor
					try {
						ObjectOutputStream oos = this.output_stream.get(current_neighbor);
						oos.writeObject(app_msg);	
						oos.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}
					//Increment total_messages_sent
					this.total_messages_sent++;
				}
			}
			//Wait for minimum sending delay before sending another message
			try {
				Thread.sleep(this.min_send_delay);
			} catch (InterruptedException e) {
				System.out.println("Error in MAP protocol");
			}
		}
		synchronized(this){
			//After sending min_per_active to max_per_active number of messages become passive
			this.active = false;
		}

	}

	//Function to generate random number in a given range
	int getRandomNumber(int min,int max) {
		return new Random().nextInt((max - min) + 1) + min;
	}
}

//Server reading objects sent by other clients in the system in a thread 
class ClientThread extends Thread {

	NodeServer obj_main;
	Socket client_socket;

	public ClientThread(Socket client_socket,NodeServer obj_main) {
		this.client_socket = client_socket;
		this.obj_main = obj_main;
	}

	public void run() {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(client_socket.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
		while(true) {
			try {
				Message msg;
				msg = (Message) ois.readObject();
				//Synchronizing obj_main so that multiple threads access obj_main in a synchronized way
				synchronized(obj_main){

					//If message is a marker message then process has to turn red if its blue and send messages along all its channels
					if(msg instanceof MarkerMsg){
						int channel_num = ((MarkerMsg) msg).node_id;
						ChandyLamport.sendMarkerMessage(obj_main,channel_num);
					}	

					//A passive process on receiving an application message only becomes active if it has sent fewer than max_number messages
					else if((obj_main.active == false) && msg instanceof ApplicationMsg && obj_main.total_messages_sent < obj_main.max_number && obj_main.logging == 0) {
						obj_main.active = true; 
						new MAPThread(obj_main).start();
					}
					//If its an application message and logging = 1 then save it
					else if((obj_main.active == false) && (msg instanceof ApplicationMsg) && (obj_main.logging == 1)) {
						//Save the channel No from where the message came from
						int channel_num = ((ApplicationMsg) msg).node_id;
						//Log the application message since logging is enabled
						ChandyLamport.logMessage(channel_num,((ApplicationMsg) msg) ,obj_main);
					}

					//If message is a state message then if this node id is 0 then process it otherwise forward it to the parent on converge cast tree towards Node 0
					else if(msg instanceof StateMsg) {
						if(obj_main.id == 0) {
							obj_main.state_messages.put(((StateMsg)msg).node_id,((StateMsg)msg));
							obj_main.allnodes_state_msg[((StateMsg) msg).node_id] = true;
							if(obj_main.state_messages.size() == obj_main.num_of_nodes) {
								System.out.println("State messages are received at node 0");
								boolean restart_cl = ChandyLamport.processStateMessages(obj_main,((StateMsg)msg));
								if(restart_cl) {
									obj_main.initialize();
									new CLThread(obj_main).start();	
								}								
							}
						}
						else{
							ChandyLamport.forwardToParent(obj_main,((StateMsg)msg));
						}
					}
					//If a finishMsg is received then forward the message to all its neighbors
					else if(msg instanceof FinishMsg) {
						ChandyLamport.sendFinishMsg(obj_main);
					}

					if(msg instanceof ApplicationMsg) {
						//Code for current_time_stamp protocol
						for(int i=0;i<obj_main.num_of_nodes;i++)
							obj_main.current_time_stamp[i] = Math.max(obj_main.current_time_stamp[i], ((ApplicationMsg) msg).current_time_stamp[i]);
						obj_main.current_time_stamp[obj_main.id]++;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

//Thread to start chandy lamport protocol
class CLThread extends Thread {

	NodeServer obj_main;

	public CLThread(NodeServer obj_main) {
		this.obj_main = obj_main;
	}

	public void run(){
		//If its the first time calling chandy Lamport protocol, start immediately
		if(obj_main.is_first_time) {
			obj_main.is_first_time = false;
		}
		//If its not first time , start after the snapShot delay
		else {
			try {
				Thread.sleep(obj_main.snapshot_delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//Irrespective of first or second time we start the protocol if this thread is started
		ChandyLamport.startSnapshotProtocol(obj_main);
	}
}

//Thread to start MAP protocol
class MAPThread extends Thread {

	NodeServer obj_main;

	public MAPThread(NodeServer obj_main) {
		this.obj_main = obj_main;
	}

	public void run() {
		try {
			obj_main.startMAPProtocol();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}