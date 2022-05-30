import java.net.*;
import java.util.ArrayList;
import java.util.TreeMap;
import java.io.*;

public class COMP3100_Assignment2 {
    
    //variable declarations.
    static Socket s;
    static DataOutputStream dout;
    static BufferedReader din;
    static String str;
    static String[] jobStrings;

    public static void main(String[] args) throws Exception {
        s = new Socket("localhost",50000);  
        din = new BufferedReader(new InputStreamReader(s.getInputStream())); 
        dout = new DataOutputStream(s.getOutputStream());  
        
        /* COMMAND:
        
                ./ds-server -c '/home/erik/Documents/ds-sim/configs/sample-configs/ds-sample-config01.xml' -v brief -n
            
            or for more info:
                
                ./ds-server -c '/home/erik/Documents/ds-sim/configs/sample-configs/ds-sample-config01.xml' -v all -n
        
            or for stage2 tests:

                ./stage2-test-x86 "java COMP3100_Assignment2" -o tt -n
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
                continue; //skip this loop instance.
            }
            
            //if a new job is ready
            if(jobCommand.equals("JOBN")) {
                schedule();
            }

            //if there are no more jobs for scheduling.
            if(jobCommand.equals("NONE")) {
                x = 0; //exit the loop, and quit.
            }

            //no jobs in queue, but jobs are running (do nothing but loop back to top). 
        }
        //QUIT!
        quit();
    }
    

    static void schedule() throws IOException {
        
        // WE RECIEVED A JOBN!

        jobStrings = str.split(" "); //split the job.
        String jobId = jobStrings[2]; //we need to save the id of each job.
        String coresRequired = jobStrings[jobStrings.length - 3]; //save cores required.
        String memoryRequired = jobStrings[jobStrings.length - 2]; //save memory required.
        String disksRequired = jobStrings[jobStrings.length - 1]; //save disks required.

        //REQUEST SERVERS
        push("GETS Avail " + coresRequired + " " + memoryRequired + " " + disksRequired);

        //RECIEVE THE DATA [number] [length of characters].
        recieve();
        int totalServers = Integer.parseInt(str.split(" ")[1]); //save total servers from response.
        System.out.println(" ---<>--- There should be: " + str.split(" ")[1] + " number of servers ---<>--- \n");

        //SEND 'OK'
        push("OK");

        //set server variables.
        String identification = "";
        int fit = -1;
        ArrayList<String> servers = new ArrayList();

        //if avaliable servers is zero - then we need to use get capable!
        if(totalServers == 0){
            recieve();
            
            //REQUEST SERVERS
            push("GETS Capable " + coresRequired + " " + memoryRequired + " " + disksRequired);

            //RECIEVE THE DATA [number] [length of characters].
            recieve();
            totalServers = Integer.parseInt(str.split(" ")[1]); //save total servers from response.

            //SEND 'OK'
            push("OK");

            for(int i = 0; i < totalServers; i++){
                recieve(); //recieve each server, and then split it into its important components.
    
                String[] serverInformation = str.split(" ");
                String type = serverInformation[0]; //save the type.
                String id = serverInformation[1]; //save the ID.
                String cores = serverInformation[serverInformation.length - 3];
                String identity = type + ":" + id + ":" + cores; //this is the servers identity.
    
                System.out.println("identity: " + identity);
    
                //add all the servers!
                servers.add(identity);
            }

            push("OK"); //after recieving all of the servers!

            recieve(); //recieve "."

            //now we need to figure out which server has the smallest amount of time before job will be run (sum of executionTime)

            Integer smallestRunTime = -1;
            for(int i = 0; i < servers.size(); i++){
                String[] serverInformation = servers.get(i).split(":");
                String type = serverInformation[0];
                String id = serverInformation[1];
                
                //find all of the assigned jobs and their execution time (includes currently running job)..
                push("LSTJ " + type + " " + id);

                //recieve amount.
                recieve();
                int amountServers = Integer.parseInt(str.split(" ")[1]); //save total servers from response.

                push("OK");

                int totalRunTime = 0; //save total run time for this server
                for(int e = 0; e < amountServers; e++){
                    recieve();
                    System.out.println("str. split[4]: " + str.split(" ")[4]);
                    totalRunTime = totalRunTime + Integer.parseInt(str.split(" ")[4]);
                }

                //if total run time for this server is less than the smallest run time, set this to be identification (will be scheduled next).
                if(smallestRunTime == -1 || totalRunTime < smallestRunTime) {
                    smallestRunTime = totalRunTime; //set the new smallest run time.
                    identification = servers.get(i); //set identification.
                }
                
                push("OK");
                //recieve "."
                recieve();
            }

        } else {
            for(int i = 0; i < totalServers; i++){
                recieve(); //recieve each server, and then split it into its important components.
    
                String[] serverInformation = str.split(" ");
                String type = serverInformation[0]; //save the type.
                String id = serverInformation[1]; //save the ID.
                String cores = serverInformation[serverInformation.length - 3];
                String identity = type + ":" + id + ":" + cores; //this is the servers identity.
    
                System.out.println("identity: " + identity);
    
                //find the smallest difference.
                int currentFit = Integer.parseInt(cores) - Integer.parseInt(coresRequired);
                
                //not been set or this fit better than saved fit.
                if(fit == -1 || currentFit < fit) {
                    fit = currentFit;
                    identification = identity;
                }
            }
            push("OK"); //after recieving all of the servers!

            recieve(); //recieve "." 
        }

        

        /*
            There are now 2 steps:
        */

        // [1] SCHEDULE THE JOB.

        //we now have the smallest server identification name.
        String[] serverInformation = identification.split(":");
        String type = serverInformation[0];
        String id = serverInformation[1];             

        //push the schedule.
        push("SCHD " + jobId + " " + type + " " + id);


        // [2] RECIEVE CONFIRMATION
        
        recieve();

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
