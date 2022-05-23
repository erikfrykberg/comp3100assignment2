import java.net.*;
import java.util.ArrayList;
import java.util.TreeMap;
import java.io.*;

public class COMP3100_ASS2 {
    
    //variable declarations.
    static Socket s;
    static DataOutputStream dout;
    static BufferedReader din;
    static String str;
    static String[] jobStrings;
    static int currentCount = 0;
    static String jobId; //we need to save the id of each job.
    static TreeMap<String, Integer> identityCounter = new TreeMap<>(); // SERVER VARIABLES [IDENTITY - "TYPE:ID"] -> [COUNT].
    
    public static void main(String[] args) throws Exception {
        s = new Socket("localhost",50000);  
        din = new BufferedReader(new InputStreamReader(s.getInputStream())); 
        dout = new DataOutputStream(s.getOutputStream());  
        
        /* COMMAND:
        
                ./ds-server -c '/home/erik/Documents/ds-sim/configs/sample-configs/ds-sample-config01.xml' -v brief -n
            
            or for more info:
                
                ./ds-server -c '/home/erik/Documents/ds-sim/configs/sample-configs/ds-sample-config01.xml' -v all -n
        */

        // CONFIRM CONNECTION WITH CONSOLE!
        System.out.println("Server started + established connection correctly!\n");

        // HANDSHAKE PROTOCOL!
        handshake("erik");

        /**
         * 
         *  LOOP THROUGH THE JOBS.
         * 
        */

        // VARIABLE DEFINITIONS
        str = ""; // for the server corespondence.

        //WHILE THERE ARE JOBS TO SCHEDULE.
        int x = 1;
        while(x == 1) {

            //SEND REDY (for jobs)
            push("REDY");

            //EXPECT JOBN or NONE RESPONSE
            recieve();

            //if there are no more jobs, we must exit.
            String jobCommand = str.split(" ")[0];
            //if a job completed.
            if(jobCommand.equals("JCPL")) {
                continue;
            }
            
            if(jobCommand.equals("JOBN")) {
                schedule();
            }

            if(jobCommand.equals("NONE")) {
                x = 0; //exit the loop, and quit.
            }

        }
        //QUIT!
        quit();
    }
    

    static void schedule() throws IOException {
        
        // WE RECIEVED A JOBN!
        jobStrings = str.split(" "); //split the job.
        jobId = jobStrings[2]; //set job id.
        String coresRequired = jobStrings[jobStrings.length - 3]; //save cores required.
        String memoryRequired = jobStrings[jobStrings.length - 2]; //save memory required.
        String disksRequired = jobStrings[jobStrings.length - 1]; //save disks required.


        //REQUEST SERVERS
        push("GETS Capable " + coresRequired + " " + memoryRequired + " " + disksRequired);


        //RECIEVE THE DATA [number] [length of characters].
        recieve();
        int totalServers = Integer.parseInt(str.split(" ")[1]); //save total servers from response.
        System.out.println(" ---<>--- There should be: " + str.split(" ")[1] + " number of servers ---<>--- \n");


        //SEND 'OK'
        push("OK");


        //set server variables.
        String smallestIdentification = "";
        int lowestIndex = -1;
        //RECIEVE THE SERVERS - MUST BE AT LEAST 1!
        for(int i = 0; i < totalServers; i++){
            recieve(); //recieve each server, and then split it into its important components.

            String[] serverInformation = str.split(" ");
            String type = serverInformation[0]; //save the type.
            String id = serverInformation[1]; //save the ID.
            String identity = type + ":" + id; //this is the servers identity.

            System.out.println("identity: " + identity);

            //if the server is null, it hasn't been used - so we will use it straight away (and it will be the smallest, since its ordered!).
            if(identityCounter.get(identity) == null) {
                if(smallestIdentification == ""){
                    //schedule the server
                    smallestIdentification = identity;
                }
            } else {
                //it already exists in the identityCounter, so check if it is lower than the current lower index.
                int count = identityCounter.get(identity);
                if(count < lowestIndex || lowestIndex == -1) {
                    //we need to set the two variables.
                    lowestIndex = count;
                    smallestIdentification = identity;
                }
            }
        }


        push("OK"); //after recieving all of the servers!


        recieve();

        // by the end of the for loop, we have the smallestIdentification required. There are now 3 steps.

        // [1] SCHEDULE THE JOB.
        
        //we now have the smallest server identification name.
        String[] serverInformation = smallestIdentification.split(":");
        System.out.println("----------------------- smallestIdentification: " + smallestIdentification);
        String type = serverInformation[0];
        String id = serverInformation[1];             

        //push the schedule.
        push("SCHD " + jobId + " " + type + " " + id);

        // [2] increase count of this server.

        identityCounter.put(smallestIdentification, currentCount);

        // [3] increase the current count.

        currentCount = currentCount + 1;

        //increase the current count and for the 
        currentCount++;

        recieve();

        System.out.println("=======================================================");
    }


    /**
     * Initiates the handshake protocol with the server.
     * @param account - the account that will be used to authorise the connection.
     */
    static void handshake(String account) throws IOException {
        push("HELO");

        //EXPECT "OK"
        recieve();

        //SEND AUTH
        push("AUTH " + account);

        //EXPECT 'OK'
        recieve();
    }
    
    /**
     * Sets the largest server.
     */
    static void getCapableServers() throws IOException {
        String coresRequired = jobStrings[jobStrings.length - 3]; //save cores required.
        String memoryRequired = jobStrings[jobStrings.length - 2]; //save memory required.
        String disksRequired = jobStrings[jobStrings.length - 1]; //save disks required.

        //REQUEST SERVERS
        push("GETS Capable " + coresRequired + " " + memoryRequired + " " + disksRequired);

        //RECIEVE THE DATA [number] [length of characters].
        recieve();
        Integer totalServers = Integer.parseInt(str.split(" ")[1]); //save total servers from response.
        System.out.println(" ---<>--- There should be: " + str.split(" ")[1] + " number of servers ---<>--- \n");

        //SEND 'OK'
        push("OK");

        int mostCores = 0; //local variable for current most cores.
        //RECIEVE THE SERVERS - MUST BE AT LEAST 1!
        for(int i = 0; i < totalServers; i++){
            recieve();
            String[] serverInformation = str.split(" ");
            Integer cores = Integer.parseInt(serverInformation[4]);

            //bigger than the old one.. i.e. 16 over 4
            // if(cores > mostCores){
            //     largestType = serverInformation[0]; //set to the biggest type
            //     numberOfServers = 0; //reset the number of servers.
            //     mostCores = cores; //set the new mostCores.
            // }
            // //increase number of servers if of the same type.
            // if(serverInformation[0].equals(largestType)){
            //     numberOfServers++;
            // }
        }

        //push OK after recieving all of the servers.
        push("OK");

        //RECIEVE '.'
        recieve();
    }

    /**
     * Quits the connection with the server.
     */
    static void quit() throws IOException {
        push("QUIT");
        dout.flush();

        str = din.readLine();
        System.out.println("Connection Closed.." + "\u001B[0m");

        dout.close();  
        s.close();  
        
    }
    
    /**
     * Recieves messages from the socket and prints them to the console.
     */
    static void recieve() throws IOException {
        str = din.readLine();
        System.out.println("RCVD: \'" + str + "\'\n");
    }

    /**
     * Pushes messages to the socket to send to the server.
     * @param string - the message to be sent.
     */
    static void push(String str) throws IOException {
        str = str + "\n";
        dout.write((str).getBytes());
        System.out.println("SENT: " + str);
        dout.flush();
    }

}
