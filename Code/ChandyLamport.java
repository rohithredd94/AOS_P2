import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;


public class ChandyLamport {
    //method where protocol starts 
	public static void startSnapshotProtocol(ProjectMain obj_main) {
		synchronized(obj_main){
			// node 0 calls this method to initiate chandy and lamport protocol
			//allnodes_state_msg is array which holds the status of receivedStateMessage from all the nodes in the system
			obj_main.allnodes_state_msg[obj_main.id] = true;
			//It turns red and sends marker messages to all its outgoing channels
			sendMarkerMessage(obj_main,obj_main.id);
		}
	}

	public static void sendMarkerMessage(ProjectMain obj_main, int channel_num){
		// Node which receives marker message turns red ,becomes passive and sends
		// marker messages to all its outgoing channels , starts logging
		synchronized(obj_main){
			if(obj_main.is_blue){
				System.out.println("Received first Marker message from node and color is blue, " + "will be changed to red  "+channel_num);
				obj_main.rec_marker.put(channel_num, true);
				obj_main.is_blue = false;
				obj_main.my_state.active = obj_main.active;
				obj_main.my_state.current_time_stamp = obj_main.current_time_stamp;
				obj_main.my_state.node_id = obj_main.id;
				System.out.println("Node "+obj_main.id+" is sending the following timestamp to Node 0");
//				for(ArrayList<ApplicationMsg> a:obj_main.in_transit_msgs.values()){
					//System.out.println("******Checking if obj_main has empty channel state:"+a.isEmpty());
//				}
//				for(int k:obj_main.my_state.current_time_stamp){
//					System.out.print(k+" ");
//				}
				int[] vector_copy = new int[obj_main.my_state.current_time_stamp.length];
				for(int i=0;i<vector_copy.length;i++){
					vector_copy[i] = obj_main.my_state.current_time_stamp[i]; 
				}
//				synchronized(obj_main.output){
				obj_main.output.add(vector_copy);
//				}
//				new writeToOutputThread(obj_main).start();
				//logging = 1 demands the process to log application messages after it has become red
				obj_main.logging = 1;
				//Send marker messages to all its neighbors
				for(int i : obj_main.neighbors){
					MarkerMsg m = new MarkerMsg();
					System.out.println("To Node "+i+" process "+obj_main.id+"  is sending marker messages now");
					m.node_id = obj_main.id;
					ObjectOutputStream oos = obj_main.output_stream.get(i);
					try {
						oos.writeObject(m);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if((obj_main.neighbors.length == 1) && (obj_main.id!=0)){
					int parent = ConvergeCast.getParent(obj_main.id);	
					obj_main.my_state.in_transit_msgs = obj_main.in_transit_msgs;
					obj_main.is_blue = true;
					obj_main.logging = 0;
					// Send channel state to parent 
					ObjectOutputStream oos = obj_main.output_stream.get(parent);
					System.out.println("Sending State Msg  by  "+obj_main.id+" and process state is  "+obj_main.my_state.active);
					try {
						oos.writeObject(obj_main.my_state);
					} catch (IOException e) {
						e.printStackTrace();
					}
					obj_main.initialize(obj_main);
				}


			}
			//If color of the process is red and a marker message is received on this channel
			else if(!obj_main.is_blue){
				System.out.println("Received a marker message when the color of process "+obj_main.id+" is red");
				// Record that on this channel a marker message was received
				obj_main.rec_marker.put(channel_num, true);
				int i=0;
				//Check if this node has received marker messages on all its incoming channels
				System.out.println("Size of the neighbors list is "+obj_main.neighbors.length);
				while(i<obj_main.neighbors.length && obj_main.rec_marker.get(obj_main.neighbors[i]) == true){
					System.out.println("Received Marker msg from neighbor "+obj_main.neighbors[i]);
					i++;
				}
				// If this node has received marker messages from all its incoming channels then 
				// send process state to Node 0
				if(i == obj_main.neighbors.length && obj_main.id != 0) {
					int parent = ConvergeCast.getParent(obj_main.id);				
					System.out.println("For node "+obj_main.id + ", all neighbours have sent marker messages.");
					// Record the channelState and process State and which node is sending to node 0 as node_id
					obj_main.my_state.in_transit_msgs = obj_main.in_transit_msgs;
//					for(ArrayList<ApplicationMsg> a:obj_main.in_transit_msgs.values()){
						//System.out.println("Checking if obj_main has empty channel state:"+a.isEmpty());
//					}
					obj_main.is_blue = true;
					obj_main.logging = 0;
					// Send channel state to parent 
					ObjectOutputStream oos = obj_main.output_stream.get(parent);
					System.out.println("Sending State Msg  by  "+obj_main.id+" and process state is  "+obj_main.my_state.active);
					try {
						oos.writeObject(obj_main.my_state);
					} catch (IOException e) {
						e.printStackTrace();
					}
					obj_main.initialize(obj_main);
				}
				if(i == obj_main.neighbors.length &&  obj_main.id == 0){
					System.out.println("For node 0, all neighbours have sent marker messages.");
					obj_main.my_state.in_transit_msgs = obj_main.in_transit_msgs;
					obj_main.state_messages.put(obj_main.id, obj_main.my_state);
					obj_main.is_blue = true;
					obj_main.logging = 0;
				}
//				if(i != obj_main.neighbors.length){
					//System.out.println("For node "+obj_main.id + ", neighbor " + obj_main.neighbors[i] 
//							+" has not yet sent a marker message.");
//				}
//				

			}
		}
	}

	// This method is called only by node 0 
	public static boolean processstate_messages(ProjectMain obj_main, StateMsg msg) throws InterruptedException {
		int i=0,j=0,k=0;
		synchronized(obj_main){
			// Check if node 0 has received state message from all the nodes in the graph
			while(i<obj_main.allnodes_state_msg.length && obj_main.allnodes_state_msg[i] == true){
				i++;
			}
			//If it has received all the state messages 
			if(i == obj_main.allnodes_state_msg.length){
				//Go through each state message
				for(j=0;j<obj_main.state_messages.size();j++){
					// Check if any process is still active , if so then no further check required 
					//wait for snapshot delay and restart snapshot protocol
					if(obj_main.state_messages.get(j).active == true){
						System.out.println(" *****************Process is still active ");
						return true;
					}
				}
				//If all processes are passive then j is now equal to num_of_nodes 
				if(j == obj_main.num_of_nodes){
					//now check for channels 
					for(k=0;k<obj_main.num_of_nodes;k++){
						// If any process has non-empty channel,  then wait for snapshot 
						// delay and restart snapshot protocol
						StateMsg value = obj_main.state_messages.get(k);
						for(ArrayList<ApplicationMsg> g:value.in_transit_msgs.values()){
							if(!g.isEmpty()){
								System.out.println("************** Channels are not empty "+k);
//								for(ApplicationMsg m:g)
									//System.out.println(m.node_id);
								//If channels are not empty immediately return, restart CL protocol is true
								return true;
							}
						}
					}
				}
				//If the above check has passed then it means all channels are empty and all processes are 
				//passive and now node 0 can announce termination - it can a send finish message to all its neighbors
				if(k == obj_main.num_of_nodes){
					System.out.println("Node 0 is sending finish message since all processes are passive and channels empty");					
					sendFinishMsg(obj_main);
					return false;
				}
			}
		}
		return false;
	}


	//When logging is enabled save all the application messages sent on each channel
	//Array list holds the application messages received on each channel
	public static void logMessage(int channel_num,ApplicationMsg m, ProjectMain obj_main) {
		synchronized(obj_main){
			// if the ArrayList is already there just add this message to it 
			if(!(obj_main.in_transit_msgs.get(channel_num).isEmpty()) && obj_main.rec_marker.get(channel_num) != true){
				obj_main.in_transit_msgs.get(channel_num).add(m);
			}
			// or create a list and add the message into it
			else if((obj_main.in_transit_msgs.get(channel_num).isEmpty()) && obj_main.rec_marker.get(channel_num) != true){
				ArrayList<ApplicationMsg> msgs = obj_main.in_transit_msgs.get(channel_num);
				msgs.add(m);
				obj_main.in_transit_msgs.put(channel_num, msgs);
			}
		}
	}

	// A process received a state msg on its channel and the process is not Node 0
	// therefore simply forward it over converge cast tree towards Node 0
	public static void forwardToParent(ProjectMain obj_main, StateMsg stateMsg) {
		synchronized(obj_main){
			int parent = ConvergeCast.getParent(obj_main.id);
			// Send stateMsg to the parent
			ObjectOutputStream oos = obj_main.output_stream.get(parent);
			try {
				oos.writeObject(stateMsg);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	//Method to send finish message to all the neighbors of the current Node
	public static void sendFinishMsg(ProjectMain obj_main) {
		synchronized(obj_main){
			new OutputWriter(obj_main).writeToFile();
			for(int s : obj_main.neighbors){
				FinishMsg m = new FinishMsg();
				ObjectOutputStream oos = obj_main.output_stream.get(s);
				try {
					oos.writeObject(m);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}			
			System.exit(0);
		}
	}
}

//Print the output to the output File
class OutputWriter {
	ProjectMain obj_main;

	public OutputWriter(ProjectMain obj_main) {
		this.obj_main = obj_main;
	}


	public void writeToFile() {
		String fileName = ProjectMain.output_file_name+"-"+obj_main.id+".out";
		synchronized(obj_main.output){
			try {
				File file = new File(fileName);
				FileWriter file_writer;
				if(file.exists()){
					file_writer = new FileWriter(file,true);
				}
				else
				{
					file_writer = new FileWriter(file);
				}
				BufferedWriter buffered_writer = new BufferedWriter(file_writer);
				/*if(file.length()!=0){
                buffered_writer.write("\n");
            }*/
   
				for(int i=0;i<obj_main.output.size();i++){
					for(int j:obj_main.output.get(i)){
						buffered_writer.write(j+" ");
						
					}
					if(i<(obj_main.output.size()-1)){
	            buffered_writer.write("\n");
					}
				}			
				obj_main.output.clear();
				// Always close files.
				buffered_writer.close();
			}
			catch(IOException ex) {
				System.out.println("Error writing to file '" + fileName + "'");
				// Or we could just do this: ex.printStackTrace();
			}
		}
	}

}
