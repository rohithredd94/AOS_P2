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

@SuppressWarnings("serial")
public class ProjectMain implements Serializable  {
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
	//ArrayLintst which holds the nodes part of the distributed system 
	ArrayList<Node> nodes = new ArrayList<Node>();
	//HashMap which has node number as keys and <id,host,port> as value
	// Create all the channels in the beginning and keep it open till the end
	HashMap<Integer,Socket> channels = new HashMap<Integer,Socket>();
	// Create all the output streams associated with each socket 
	HashMap<Integer,ObjectOutputStream> output_stream = new HashMap<Integer,ObjectOutputStream>();
	// HashMap which stores ArrayList of messages recorded while the process is red for each channel
	HashMap<Integer,ArrayList<ApplicationMsg>> in_transit_msgs;
	// HashMap which stores all incoming channels and boolean received marker message
	HashMap<Integer,Boolean> rec_marker;
	// HashMap which stores all state messages
	HashMap<Integer,StateMsg> state_messages;	
	//Used to determine if state message has been received from all the processes in the system
	boolean[] allnodes_state_msg;
	//Every process stores its state(current_time_stamp,in_transit_msgs and its id) in this StateMsg Object
	StateMsg my_state;
	//To hold output Snapshots
	ArrayList<int[]> output = new ArrayList<int[]>();

	//Re-initialize everything that is needed for Chandy Lamport protocol before restarting it
	void initialize(ProjectMain obj_main){
		obj_main.in_transit_msgs = new HashMap<Integer,ArrayList<ApplicationMsg>>();
		obj_main.rec_marker = new HashMap<Integer,Boolean>();
		obj_main.state_messages = new HashMap<Integer,StateMsg>();	

		Set<Integer> keys = obj_main.channels.keySet();
		//Initialize in_transit_msgs hashMap
		for(Integer element : keys){
			ArrayList<ApplicationMsg> arrList = new ArrayList<ApplicationMsg>();
			obj_main.in_transit_msgs.put(element, arrList);
		}
		//Initialize boolean hashmap rec_marker to false
		for(Integer e: obj_main.neighbors){
			obj_main.rec_marker.put(e,false);
		}
		obj_main.allnodes_state_msg = new boolean[obj_main.num_of_nodes];
		obj_main.my_state = new StateMsg();
		obj_main.my_state.current_time_stamp = new int[obj_main.num_of_nodes];
	}


	public static void main(String[] args) throws IOException, InterruptedException {
		//Read the values for all variables from the configuration file
		ProjectMain obj_main = ConfigParser.readConfigFile(args[1]);
		// Get the node number of the current Node
		obj_main.id = Integer.parseInt(args[0]);
		int current_node = obj_main.id;
		//Get the configuration file from command line
		//String config_name = args[1];
		ProjectMain.output_file_name = args[1].substring(0, args[1].lastIndexOf('.'));
		//Build converge cast spanning tree in the beginning
		ConvergeCast.build_tree(obj_main.adj_matrix);
		// Transfer the collection of nodes from ArrayList to hash map which has node id as key since  
		// we need to get and node as value ,it returns <id,host,port> when queried with node Id.
		HashMap<Integer, Node> node_address_list = new HashMap<Integer, Node>();
		for(int i=0;i<obj_main.nodes.size();i++){
			node_address_list.put(obj_main.nodes.get(i).node_id, obj_main.nodes.get(i));
		}
		// Get the port number on which this node should listen 
		int server_port = obj_main.nodes.get(obj_main.id).port;
		// Start server on this node's assigned port
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
		for(Integer element : keys) obj_main.neighbors[index++] = element.intValue();
		//obj_main.current_time_stamp is used to maintain the current timestamp of the process
		obj_main.current_time_stamp = new int[obj_main.num_of_nodes];

		//Initialize all the datastructures needed for the node to run the protocols
		obj_main.initialize(obj_main);

		//Initially node 0 is active therefore if this node is 0 then it should be active
		if(current_node == 0){
			obj_main.active = true;
			System.out.println("Emitted Messages");			
			//Start Chandy Lamport protocol if it is node 0
			new CLThread(obj_main).start();		
			new EmitMessagesThread(obj_main).start();
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


	void emitMessages() throws InterruptedException{

		// get a random number between min_per_active to max_per_active to emit that many messages
		int num_of_msgs = 1;
		int min_send_delay = 0;
		synchronized(this){
			num_of_msgs = this.getRandomNumber(this.min_per_active,this.max_per_active);
			// If random number is 0 then since node 0 is the only process active in the beginning it will not start
			// therefore get a bigger random number
			if(num_of_msgs == 0){
				num_of_msgs = this.getRandomNumber(this.min_per_active + 1,this.max_per_active);
			}
			min_send_delay = this.min_send_delay;
		}
		//System.out.println("For Node "+this.id+ "  Random number of messages in range min - max per active is  "+num_of_msgs);
		// channels hashMap has all neighbors as keys, store them in an array to get random neighbor
		for(int i=0;i<num_of_msgs;i++) {
			synchronized(this){
				//get a random number to index in the neighbors and array and get that neighbor
				int neighbor_index = this.getRandomNumber(0,this.neighbors.length-1);
				int current_neighbor = this.neighbors[neighbor_index];
//				System.out.println("Neighbor chosen is "+current_neighbor);
				if(this.active == true){
					//send application message
					ApplicationMsg m = new ApplicationMsg(); 
					// Code for current_time_stamp protocol
					this.current_time_stamp[this.id]++;
					m.current_time_stamp = new int[this.current_time_stamp.length];
					System.arraycopy( this.current_time_stamp, 0, m.current_time_stamp, 0, this.current_time_stamp.length );
					m.node_id = this.id;
					//					System.out.println("Timestamp that is being sent while message is emitted ");
					//					for(int s:m.current_time_stamp){
					//						System.out.println(s+" ");
					//					}
					// Write the message in the channel connecting to neighbor
					try {
						ObjectOutputStream oos = this.output_stream.get(current_neighbor);
						oos.writeObject(m);	
						oos.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}	
					//increment total_messages_sent
					total_messages_sent++;
				}
			}
			// Wait for minimum sending delay before sending another message
			try {
				Thread.sleep(min_send_delay);
			} catch (InterruptedException e) {
				System.out.println("Error in EmitMessages");
			}
		}
		synchronized(this){
			// After sending min_per_active to max_per_active number of messages become passive
			this.active = false;
		}

	}

	// Function to generate random number in a given range
	int getRandomNumber(int min,int max){
		// Usually this can be a field rather than a method variable
		Random rand = new Random();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int random_number = rand.nextInt((max - min) + 1) + min;
		return random_number;
	}
}

//Server reading objects sent by other clients in the system in a thread 
class ClientThread extends Thread {
	Socket client_socket;
	ProjectMain obj_main;

	public ClientThread(Socket client_socket,ProjectMain obj_main) {
		this.client_socket = client_socket;
		this.obj_main = obj_main;
	}

	public void run() {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(client_socket.getInputStream());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		while(true){
			try {
				Message msg;
				msg = (Message) ois.readObject();
				// Synchronizing obj_main so that multiple threads access obj_main in a synchronized way
				synchronized(obj_main){

					//If message is a marker message then process has to turn red if its blue and send messages along all its
					//channels
					if(msg instanceof MarkerMsg){
						int channel_num = ((MarkerMsg) msg).node_id;
						ChandyLamport.sendMarkerMessage(obj_main,channel_num);
					}	

					//A passive process on receiving an application message only becomes active if 
					//it has sent fewer than max_number messages
					else if((obj_main.active == false) && msg instanceof ApplicationMsg && 
							obj_main.total_messages_sent < obj_main.max_number && obj_main.logging == 0){
						obj_main.active = true; 
						new EmitMessagesThread(obj_main).start();
					}
					//If its an application message and logging = 1 then save it
					else if((obj_main.active == false) && (msg instanceof ApplicationMsg) && (obj_main.logging == 1)){
						//Save the channel No from where the message came from
						int channel_num = ((ApplicationMsg) msg).node_id;
						//Log the application message since logging is enabled
						ChandyLamport.logMessage(channel_num,((ApplicationMsg) msg) ,obj_main);
					}

					//If message is a state message then if this node id is 0 then process it 
					// otherwise forward it to the parent on converge cast tree towards Node 0
					else if(msg instanceof StateMsg){
						if(obj_main.id == 0){
							//System.out.println("Received State msg at Node 0 from node "+((StateMsg)msg).node_id);
							obj_main.state_messages.put(((StateMsg)msg).node_id,((StateMsg)msg));
							obj_main.allnodes_state_msg[((StateMsg) msg).node_id] = true;
							//System.out.println("state_messages size = "+obj_main.state_messages.size());
							if(obj_main.state_messages.size() == obj_main.num_of_nodes){
								//System.out.println("State messages are received at node 0");
								boolean restart_CL = ChandyLamport.processstate_messages(obj_main,((StateMsg)msg));
								if(restart_CL){
									//System.out.println("Restarting Chandy Lamport Protocol");
									obj_main.initialize(obj_main);
									//									for(ArrayList<ApplicationMsg> a:obj_main.in_transit_msgs.values()){
									//										System.out.println("Checking if obj_main has empty channel state:"+a.isEmpty());
									//									}
									//Call Chandy Lamport protocol 
									new CLThread(obj_main).start();	
								}								
							}
						}
						else{
							//System.out.println("Forwarding state msg to my parent - node"+obj_main.id);
							ChandyLamport.forwardToParent(obj_main,((StateMsg)msg));
						}
					}
					//If a finishMsg is received then forward the message to all its neighbors
					else if(msg instanceof FinishMsg){	
						//System.out.println("Finish Message of Node "+obj_main.id+" finish message is"+((FinishMsg)msg).msg);
						ChandyLamport.sendFinishMsg(obj_main);
					}

					if(msg instanceof ApplicationMsg){
						//						System.out.println("TimeStamp when application message is received and not processed at node "+obj_main.id);
						//						for(int j: ((ApplicationMsg) msg).current_time_stamp){
						//							System.out.println(j+" ");
						//						}
						//Code for current_time_stamp protocol
						for(int i=0;i<obj_main.num_of_nodes;i++){
							obj_main.current_time_stamp[i] = Math.max(obj_main.current_time_stamp[i], ((ApplicationMsg) msg).current_time_stamp[i]);
						}
						obj_main.current_time_stamp[obj_main.id]++;
						// print the current_time_stamp 
						//						System.out.println("current_time_stamp of node id "+obj_main.id+" when appln msg is received and processed");
						//						for(int i:obj_main.current_time_stamp){
						//							System.out.println(i);
						//						}
					}
				}
			}
			catch (IOException e) {
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
class CLThread extends Thread{

	ProjectMain obj_main;
	public CLThread(ProjectMain obj_main){
		this.obj_main = obj_main;
	}
	public void run(){
		//If its the first time calling chandy Lamport protocol, start immediately
		if(obj_main.is_first_time){
			obj_main.is_first_time = false;
		}
		//If its not first time , start after the snapShot delay
		else{
			try {
				Thread.sleep(obj_main.snapshot_delay);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//Irrespective of first or second time we start the protocol if this thread is started
		ChandyLamport.startSnapshotProtocol(obj_main);
	}
}

//Thread to start chandy lamport protocol
class EmitMessagesThread extends Thread{

	ProjectMain obj_main;
	public EmitMessagesThread(ProjectMain obj_main){
		this.obj_main = obj_main;
	}
	public void run(){
		try {
			obj_main.emitMessages();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
