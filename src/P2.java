import javax.sound.midi.Soundbank;
import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by nidhi on 4/18/17.
 */


class postingListData {
    int term_tuple_id;
    int term_postion;

    public postingListData(int term_tuple_id, int term_postion) {
        this.term_postion = term_postion;
        this.term_tuple_id = term_tuple_id;
    }
}

class range {
    int maxRange;
    int minRange;

    range(int minRange, int maxRange) {
        this.maxRange = maxRange;
        this.minRange = minRange;
    }
}

public class P2 {
    private static ServerSocket tupleSocket;
    private static boolean nodeDeleted = false;

    //Store formatted_data String and count eg. ""abc"2.5" , 2
    private static HashMap<String, Integer> store_exact_data_cnt = new HashMap<String, Integer>();
    private static HashMap<String, Integer> store_exact_data_cnt_bkp = new HashMap<String, Integer>();


    //Store type String and formatted_data eg. 121 ,""abc"2.5"
    private static HashMap<String, List<String>> store_type_data = new HashMap<String, List<String>>();
    private static HashMap<String, List<String>> store_type_data_bkp = new HashMap<String, List<String>>();


    //Store tuple ID and formatted_data eg.  0 ,""abc"2.5"
    private static HashMap<Integer, String> store_id_exactData = new HashMap<Integer, String>();
    private static HashMap<Integer, String> store_id_exactData_bkp = new HashMap<Integer, String>();


    //Store tuple ID and original_data eg.  0 ,"("abc",2.5)"
    private static HashMap<Integer, String> store_id_OrigData = new HashMap<Integer, String>();
    private static HashMap<Integer, String> store_id_OrigData_bkp = new HashMap<Integer, String>();


    //Store tuple ID and formatted_data eg.  0 ,"121"
    private static HashMap<Integer, String> store_id_type = new HashMap<Integer, String>();
    private static HashMap<Integer, String> store_id_type_bkp = new HashMap<Integer, String>();


    //Store posting list of each term
    private static HashMap<String, List<postingListData>> posting_list = new HashMap<String, List<postingListData>>();
    private static HashMap<String, List<postingListData>> posting_list_bkp = new HashMap<String, List<postingListData>>();


    private static HashMap<Integer, serverDetails> connected_host_details = new HashMap<Integer, serverDetails>();

    private static HashMap<String, Integer> hostname_id_map = new HashMap<String, Integer>();

    private static TopologyInfo myTopology;
    private static serverDetails myServer;

    /// Instead of below use PartitionToNode from TopologyInfo
    private static HashMap<Integer, Integer> partition_host = new HashMap<Integer, Integer>();
    private static ArrayList<range> rangeList = new ArrayList<range>();

    private static HashMap<Integer, Integer> hostid_backuphostid = new HashMap<Integer, Integer>();
    private static HashMap<Integer, Integer> backuphostid_hostid = new HashMap<Integer, Integer>();

    private static int host_id_gererate = 0;
    private static int tuple_id = 0;
    private static String master_ip = null;
    private static int master_id;
    private static int master_port;
    private static String local_ip;
    private static int local_port;
    private static int local_id;
    private static String local_hostname;
    private static int localhost_bkpHostID;
    private static int total_host_in_ds;
    private static String hostsFilePath = null;
    private static String absolutePathTuple = null;
    private static String absolutePathBackupTuple = null;
    private static String tempFilePath = null;

    public static boolean isPortValid(int port) {
        int i = port;
        if (i > 1024 && i < 65535) {
            return true;
        } else {
            return false;
        }
    }

    public static String createDirectory(String hostName) {
        try {
            File f = new File("/tmp/nshah/linda/" + hostName);
            String flag = null;
            String flag1 = null;

            if (f.mkdirs()) {
                System.out.println("nets file successfully created");
                flag = "Success";
            } else {
                flag = "notsuccess";
            }

            if (flag == "Success") {
                File file = new File(f.getAbsolutePath() + "/nets.txt");
                if (file.createNewFile()) {
                    flag1 = "Success";
                } else {
                    flag1 = "notsuccess";
                }
                return file.getAbsolutePath();
            }
        } catch (Exception e) {
            System.out.println("createDirectory : failed");
        }
        return "NotSuccess";
    }

    public static void createTupleDirectory(String hostName) {

        List<Integer> localPartitionList = myTopology.getNodeToPartition().get(local_id);
        boolean flag = false;
        for(int i = 0;i<localPartitionList.size();i++)
        {
            //System.out.println("creating partition on local machine "+localPartitionList.get(i));
            try {

                File file1 = new File("/tmp/nshah/linda/" + hostName + "/partition_"+localPartitionList.get(i)+".txt");
                if(!file1.exists())
                {
                    if (file1.createNewFile()) {
                        flag = true;
                    } else {
                        flag = false;
                    }
                }

            } catch (Exception e) {
                System.out.println("createTupleDirectory : failed");
            }

        }
    }

    public static String createBackupTupleDirectory(String hostName) {
        try {

            File file1 = new File("/tmp/nshah/linda/" + hostName + "/backup_tuples.txt");
            if (file1.createNewFile()) {
                return file1.getAbsolutePath();
            } else {
                return "NotSuccess";
            }

        } catch (Exception e) {
            System.out.println("createBackupTupleDirectory : failed");
        }

        return "NotSuccess";

    }

    public static String getIP() {
        try {
            InetAddress ipAddr = InetAddress.getLocalHost();
            String[] serverName = ipAddr.toString().split("/");
            return serverName[1];

        } catch (UnknownHostException ex) {
            ex.printStackTrace();
            return "Invalid";
        }

    }

    public static void addHostDetails(int hostID, String hostName, String host_ip, int port, String filePath) {
        try {
            FileWriter writer = new FileWriter(filePath, true);
            writer.write(hostID + " " + hostName + " " + host_ip + " " + port);
            writer.write("\r\n");
            writer.close();
        } catch (IOException e) {
            System.out.println("addHostDetails : failed");
        }

    }

    public static void addtuples(String formatString, String filePath) {
        try {
            FileWriter writer = new FileWriter(filePath, true);
            writer.write(formatString);
            writer.write("\r\n");
            writer.close();
        } catch (IOException e) {
            System.out.println("addtuples : failed");
        }

    }

    public static void addtuples_bkp(String formatString, String filePath) {
        try {
            FileWriter writer = new FileWriter(filePath, true);
            writer.write(formatString);
            writer.write("\r\n");
            writer.close();
        } catch (IOException e) {
            System.out.println("addtuples_bkp : failed");
        }

    }

    public static int getMD5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes());
            BigInteger number = new BigInteger(1, messageDigest);
            int num = number.intValue();
            //return Math.abs(num % connected_host_details.size()); // gives a host number
            return Math.abs(num);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean createBackupHostLkp() {

        List<Integer> list = new ArrayList<Integer>();
        boolean bkpHostChange = false;
        int total_host = connected_host_details.size();
        int backupHostBefore =  localhost_bkpHostID;
        int backupHostAfter = -1;
        for (Integer key : connected_host_details.keySet()) {
            list.add(key);
        }

        System.out.println();
        System.out.println("Host ID - Backup host ID mapping");
        for (int iterator = 0; iterator < list.size(); iterator++) {
            int key = list.get(iterator);
            int backup = list.get(Math.floorMod(((iterator + 1) + list.size()), list.size()));
            hostid_backuphostid.put(key, backup);
            System.out.println("host ID " + list.get(iterator) + " backup host ID " + backup);
            if (key == local_id) {
                localhost_bkpHostID = backup;
                backupHostAfter = backup;
            }
        }

        if(backupHostBefore!=backupHostAfter){
            bkpHostChange = true;
        }
        return bkpHostChange;

    }

    public static void transferTopologyInformation(String hostIP,int hostPort)
    {   Socket connectionSocket;
        List<Integer> partitionToNode = myTopology.getPartitionToNode();


        try {
            connectionSocket = new Socket(hostIP,hostPort); //connect with requested new host
            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);


            output.println("transferTopology");
            String line = "";

            for (int iterate=0; iterate < partitionToNode.size(); iterate++) {
                output.println(iterate+" "+partitionToNode.get(iterate));
            }
            output.println("end");

            String read_response = in.readLine();
            if (read_response.equals("partitionAdded")) {
                System.out.println("parition information sent successfully");
                in.close();
                output.close();
            }

            in.close();
            output.close();
            connectionSocket.close();
        } catch (IOException e) {
            System.out.println("transferTopologyInformation : failed");
        }

    }

    public static void removeDataFromBackupHost(String tuple,String hostIP,int hostPort){
        Socket connectionSocket;
        try {
            connectionSocket = new Socket(hostIP,hostPort); //connect with requested new host
            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

            output.println("removeTupleBackup/"+tuple);

            String read_response = in.readLine();

            if (read_response.equals("removed")) {
                System.out.println("tuple "+tuple+" removed from backup host");
                in.close();
                output.close();
            }

            in.close();
            output.close();
            connectionSocket.close();
        } catch (IOException e) {
            System.out.println("removeDataFromBackupHost : failed");
        }
    }

    public static void transferPartitionToNewHost(List<Integer> partitionsToBeCopied,String hostIP,int hostPort)
    {   Socket connectionSocket;
        try {
            connectionSocket = new Socket(hostIP,hostPort); //connect with requested new host
            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

            output.println("transferPartition");

            for(int i =0;i<partitionsToBeCopied.size();i++) {
                //System.out.println("transferPartitionToNewHost : transferring partition "+partitionsToBeCopied.get(i));
                String absolutePartitionPath = "/tmp/nshah/linda/"+local_hostname+"/partition_"+partitionsToBeCopied.get(i)+".txt";
                String line = "";
                output.println("addPartition/"+partitionsToBeCopied.get(i));
                BufferedReader br = new BufferedReader(new FileReader(absolutePartitionPath));
                try {
                    while ((line = br.readLine()) != null) {
                        output.println(partitionsToBeCopied.get(i)+"/"+line);
                        removeTuple(line);
                    }

                } catch (IOException e) {
                    System.out.println("transferPartitionToNewHost : failed");
                } finally {
                    br.close();
                }

            }

            output.println("end");

            String read_response = in.readLine();
            if (read_response.equals("copied")) {
                System.out.println("parition transferred to new host successfully");
                in.close();
                output.close();
            }

            in.close();
            output.close();
            connectionSocket.close();
        } catch (IOException e) {
            System.out.println("transferPartitionToNewHost : failed");
        }




    }

    public static void clearBackupTupleDataLocal(){
        posting_list_bkp.clear();
        store_id_type_bkp.clear();
        store_type_data_bkp.clear();
        store_id_exactData_bkp.clear();
        store_id_OrigData_bkp.clear();
        store_exact_data_cnt_bkp.clear();

        try {
            Runtime r = Runtime.getRuntime();
            Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + local_hostname + "/backup_tuples.txt");
            Process changePermissions2 = r.exec("touch /tmp/nshah/linda/" + local_hostname + "/backup_tuples.txt");
            Process changePermissions3 = r.exec("chmod 777 /tmp/nshah/linda/" + local_hostname + "/backup_tuples.txt");
            changePermissions1.waitFor();
            changePermissions2.waitFor();
            changePermissions3.waitFor();
        } catch (IOException e) {
            System.out.println("clearBackupTupleDataLocal : failed");
        } catch (InterruptedException e) {
            System.out.println("clearBackupTupleDataLocal : failed");
        }

    }
    /*Method to transfer original tuple to backup host in case back up host changes during add/delete host commands*/
    public static synchronized void TransferOriginalTupleData(List<List<Integer>> partitionListToremove) {
        Socket connectionSocket;
        if(total_host_in_ds>1) {
            List<Integer> NodeToPartition = myTopology.getNodeToPartition().get(local_id);


            try {


                serverDetails serverDtls = connected_host_details.get(hostid_backuphostid.get(local_id)); //get backup host details
                connectionSocket = new Socket(serverDtls.getIp(), serverDtls.getPort()); //connect with other host
                BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
                output.println("backupHostChangeRequest");
                for (int i = 0; i < NodeToPartition.size(); i++) {
                    String filePath = "/tmp/nshah/linda/" + local_hostname + "/partition_" + NodeToPartition.get(i) + ".txt";

                    String line = "";
                    BufferedReader br = new BufferedReader(new FileReader(filePath));
                    try {
                        //send number of lines are going to transfer.
                        while ((line = br.readLine()) != null) {
                            output.println(line);
                        }


                    } catch (IOException e) {
                        System.out.println("TransferOriginalTupleData : failed");
                    } finally {
                        br.close();
                    }


                }

                output.println("end");
                Thread.sleep(1000);

                boolean close = false;
                int j = 5;
                while (j >= 0) {
                    String read_response = in.readLine();
                    if (read_response.equals("backupDataAdded")) {
                        //System.out.println("Backup tuple data transferred successfully");
                        close = true;
                        break;
                    }
                    j--;
                }
                if (!close) {
                    System.out.println("Backup tuple data transferre failed");
                }


                in.close();
                output.close();
                connectionSocket.close();

            } catch (IOException e) {
                System.out.println("TransferOriginalTupleData : failed");
            } catch (InterruptedException e) {
                System.out.println("TransferOriginalTupleData : failed");
            }

            if (partitionListToremove.size() != 0) {
                for (int i = 0; i < partitionListToremove.size(); i++) {
                    List<Integer> totalpartitionListToTransfer = partitionListToremove.get(i);
                    for (int partition = 0; partition < totalpartitionListToTransfer.size(); partition++) {
                        try {
                            int partitionID = totalpartitionListToTransfer.get(partition);
                            Runtime r = Runtime.getRuntime();
                            Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + local_hostname + "/partition_" + partitionID + ".txt");
                            changePermissions1.waitFor();

                        } catch (Exception e) {
                            System.out.println("TransferOriginalTupleData : failed");
                        }
                    }
                }
            }
        }
       /* else if(total_host_in_ds == 1){
            try {
                Runtime r = Runtime.getRuntime();
                Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + local_hostname + "/backup_tuples.txt");
                Process changePermissions2 = r.exec("touch /tmp/nshah/linda/" + local_hostname + "/backup_tuples.txt");
                changePermissions1.waitFor();
                changePermissions2.waitFor();

            } catch (Exception e) {
                System.out.println("TransferOriginalTupleData : failed");
            }
            List <Integer> partList = myTopology.getNodeToPartition().get(local_id);
            for( Map.Entry<Integer,String > it : store_id_OrigData.entrySet()){
                String tuple_data = "("+it.getValue()+")";
                addtuples_bkp(tuple_data,absolutePathBackupTuple);
            }
        }*/

    }

    public static void removePartitionDataFromBackupHost(List<List<Integer>> partitionList) {
        Socket connectionSocket;
        for (int i = 0; i < partitionList.size(); i++)
        {  List<Integer> totalpartitionListToTransfer = partitionList.get(i);
            for (int partition =0;partition <totalpartitionListToTransfer.size();partition++)
            {
                String filePath = "/tmp/nshah/linda/" + local_hostname + "/partition_" + totalpartitionListToTransfer.get(partition) + ".txt";
                try {
                    serverDetails serverDtls = connected_host_details.get(hostid_backuphostid.get(local_id)); //get backup host details
                    connectionSocket = new Socket(serverDtls.getIp(), serverDtls.getPort()); //connect with other host
                    BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                    String line = "";
                    BufferedReader br = new BufferedReader(new FileReader(filePath));
                    try {
                        output.println("backupHostDeleteTuple"); //send number of lines are going to transfer.
                        while ((line = br.readLine()) != null) {
                            output.println(line);
                        }
                        output.println("end");
                        Thread.sleep(5000);

                    } catch (IOException e) {
                        System.out.println("removePartitionDataFromBackupHost : failed");
                    } catch (InterruptedException e) {
                        System.out.println("removePartitionDataFromBackupHost : failed");
                    } finally {
                        br.close();
                    }

                    boolean close = false;
                    int j = 5;
                    while (j >= 0) {
                        String read_response = in.readLine();
                        if (read_response.equals("removed")) {
                            try {
                                int partitionID = totalpartitionListToTransfer.get(partition);
                                Runtime r = Runtime.getRuntime();
                                Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + local_hostname + "/partition_"+partitionID+".txt");
                                changePermissions1.waitFor();

                            }catch (Exception e){
                                System.out.println("removePartitionDataFromBackupHost : failed");
                            }
                            System.out.println("tuple data removed from backup host successfully");
                            close = true;
                            break;
                        }
                        j--;
                    }
                    if (!close) {
                        System.out.println("Backup tuple data transferre failed");
                    }
                    in.close();
                    output.close();
                    connectionSocket.close();
                } catch (IOException e) {
                    System.out.println("removePartitionDataFromBackupHost : failed");
                }
            }
        }

    }

    public static String[] formatString(String str, int tuple_id) {
        String[] formatted_string = new String[3];
        String st = str.substring(str.indexOf("("), str.indexOf(")") + 1);
        String stringssplit = str.substring(str.indexOf("(") + 1, str.indexOf(")"));
        String[] split_data = stringssplit.split("\\,");
        String result;
        StringBuilder listString_data = new StringBuilder();
        StringBuilder listString_type = new StringBuilder();
        int position = -1;

        for (String in : split_data) {
            position = position + 1;
            String set = null;
            String final_string = null;

            Scanner scanner = new Scanner(in);
            //String -- 1 , int---2
            if (in.matches(".*[a-zA-Z]+.*") | in.contains("“")) {
                if (!in.contains("?")) {
                    set = "1";
                    final_string = in.trim();
                } else if (in.contains("?") && in.toLowerCase().contains(":int")) {
                    set = "2";
                    final_string = in.trim();
                } else if (in.contains("?") && in.toLowerCase().contains(":string")) {
                    set = "1";
                    final_string = in.trim();
                } else if (in.contains("?") && (in.toLowerCase().contains(":float") | in.toLowerCase().contains(":double"))) {
                    set = "3";
                    final_string = in.trim();
                }


            } else if (scanner.hasNextInt() | (in.contains("?") | in.toLowerCase().contains(":int"))) {
                set = "2";
                final_string = in.trim();

            } else if (scanner.hasNextFloat() | scanner.hasNextDouble() | (in.contains("?") | in.toLowerCase().contains(":float"))) {
                set = "3";
                final_string = in.trim();

            }


            listString_type.append(set);          //concate data type
            listString_data.append(final_string); //concate String

            if (tuple_id != -1) {
                if (posting_list.containsKey(final_string)) {
                    posting_list.get(final_string).add(new postingListData(tuple_id, position));
                } else {
                    List<postingListData> postingData = new ArrayList<postingListData>();
                    postingData.add(new postingListData(tuple_id, position));
                    posting_list.put(final_string, postingData); // final_string contains Terms
                }
            }

            scanner.close();

        }

        formatted_string[0] = listString_type.toString(); //0 --type
        formatted_string[1] = listString_data.toString(); //1 -- data
        formatted_string[2] = stringssplit;

        if (tuple_id != -1) {
            if (store_type_data.containsKey(listString_type.toString())) {
                store_type_data.get(listString_type.toString()).add(st);
            } else {
                List<String> data = new ArrayList<String>();
                data.add(st);
                store_type_data.put(listString_type.toString(), data);
            }

            store_id_type.put(tuple_id, formatted_string[0]); //Store Type
            store_id_exactData.put(tuple_id, formatted_string[1]); //Store formatted concate string
            store_id_OrigData.put(tuple_id, formatted_string[2]); // store orig data

            if (store_exact_data_cnt.containsKey(formatted_string[1])) {
                int i = store_exact_data_cnt.get(formatted_string[1]);
                store_exact_data_cnt.put(formatted_string[1], ++i);
            } else {
                store_exact_data_cnt.put(formatted_string[1], 1);
            }
        }
        return formatted_string;
    }

    /**/

    public static String[] formatString_bkp(String str, int tuple_id) {
        String[] formatted_string = new String[3];
        String st = str.substring(str.indexOf("("), str.indexOf(")") + 1);
        String stringssplit = str.substring(str.indexOf("(") + 1, str.indexOf(")"));
        String[] split_data = stringssplit.split("\\,");
        String result;
        StringBuilder listString_data = new StringBuilder();
        StringBuilder listString_type = new StringBuilder();
        int position = -1;

        for (String in : split_data) {
            position = position + 1;
            String set = null;
            String final_string = null;

            Scanner scanner = new Scanner(in);
            //String -- 1 , int---2
            if (in.matches(".*[a-zA-Z]+.*") | in.contains("“")) {
                if (!in.contains("?")) {
                    set = "1";
                    final_string = in.trim();
                } else if (in.contains("?") && in.toLowerCase().contains(":int")) {
                    set = "2";
                    final_string = in.trim();
                } else if (in.contains("?") && in.toLowerCase().contains(":string")) {
                    set = "1";
                    final_string = in.trim();
                } else if (in.contains("?") && (in.toLowerCase().contains(":float") | in.toLowerCase().contains(":double"))) {
                    set = "3";
                    final_string = in.trim();
                }


            } else if (scanner.hasNextInt() | (in.contains("?") | in.toLowerCase().contains(":int"))) {
                set = "2";
                final_string = in.trim();

            } else if (scanner.hasNextFloat() | scanner.hasNextDouble() | (in.contains("?") | in.toLowerCase().contains(":float"))) {
                set = "3";
                final_string = in.trim();

            }


            listString_type.append(set);          //concate data type
            listString_data.append(final_string); //concate String

            if (tuple_id != -1) {
                if (posting_list_bkp.containsKey(final_string)) {
                    posting_list_bkp.get(final_string).add(new postingListData(tuple_id, position));
                } else {
                    List<postingListData> postingData = new ArrayList<postingListData>();
                    postingData.add(new postingListData(tuple_id, position));
                    posting_list_bkp.put(final_string, postingData); // final_string contains Terms
                }
            }

            scanner.close();

        }

        formatted_string[0] = listString_type.toString(); //0 --type
        formatted_string[1] = listString_data.toString(); //1 -- data
        formatted_string[2] = stringssplit;

        if (tuple_id != -1) {
            if (store_type_data_bkp.containsKey(listString_type.toString())) {
                store_type_data_bkp.get(listString_type.toString()).add(st);
            } else {
                List<String> data = new ArrayList<String>();
                data.add(st);
                store_type_data_bkp.put(listString_type.toString(), data);
            }

            store_id_type_bkp.put(tuple_id, formatted_string[0]); //Store Type
            store_id_exactData_bkp.put(tuple_id, formatted_string[1]); //Store formatted concate string
            store_id_OrigData_bkp.put(tuple_id, formatted_string[2]); // store orig data

            if (store_exact_data_cnt_bkp.containsKey(formatted_string[1])) {
                int i = store_exact_data_cnt_bkp.get(formatted_string[1]);
                store_exact_data_cnt_bkp.put(formatted_string[1], ++i);
            } else {
                store_exact_data_cnt_bkp.put(formatted_string[1], 1);
            }
        }

        return formatted_string;
    }

    public static void removeTuple(String str) {
        int tupleIDFound = -1;
        String[] formatted_string = new String[3];
        formatted_string = formatString(str, -1);
        String[] parsedData = formatted_string[2].split("\\,");
        HashSet<Integer> findTupleID = new HashSet<Integer>();

        if (store_exact_data_cnt.containsKey(formatted_string[1])) {
            if (store_exact_data_cnt.get(formatted_string[1]) > 1) {
                store_exact_data_cnt.put(formatted_string[1], store_exact_data_cnt.get(formatted_string[1]) - 1);
            } else {
                store_exact_data_cnt.remove(formatted_string[1]);
            }
        }

        if (store_type_data.containsKey(formatted_string[0])) {
            List<String> fetch = store_type_data.get(formatted_string[0]);
            for (String s : fetch) {
                if (s.equals(str)) {
                    fetch.remove(s);
                    break;
                }
            }
            if (fetch.size() == 0) {
                store_type_data.remove(formatted_string[0]);
            }
        }

        for (Integer it : store_id_OrigData.keySet()) {
            if (formatted_string[2].equals(store_id_OrigData.get(it))) {
                tupleIDFound = it;
                break;
            }

        }

        for (int i = 0; i < parsedData.length; i++) {
            if (posting_list.containsKey(parsedData[i].trim())) {
                List<postingListData> l = posting_list.get(parsedData[i].trim());
                for (postingListData posting : l) {
                    if (tupleIDFound == posting.term_tuple_id) {
                        l.remove(posting);
                        break;
                    }
                }
                if (l.size() == 0) {
                    posting_list.remove(parsedData[i].trim());
                }
            }

        }

        store_id_type.remove(tupleIDFound);
        store_id_OrigData.remove(tupleIDFound);
        store_id_exactData.remove(tupleIDFound);

        System.out.println("removed tuple ID " + tupleIDFound + " and data " + str);

    }

    public static void removeTuple_bkp(String str) {
        int tupleIDFound = -1;
        String[] formatted_string = new String[3];
        formatted_string = formatString_bkp(str, -1);
        String[] parsedData = formatted_string[2].split("\\,");
        HashSet<Integer> findTupleID = new HashSet<Integer>();

        if (store_exact_data_cnt_bkp.containsKey(formatted_string[1])) {
            if (store_exact_data_cnt_bkp.get(formatted_string[1]) > 1) {
                store_exact_data_cnt_bkp.put(formatted_string[1], store_exact_data_cnt_bkp.get(formatted_string[1]) - 1);
            } else {
                store_exact_data_cnt_bkp.remove(formatted_string[1]);
            }
        }

        if (store_type_data_bkp.containsKey(formatted_string[0])) {
            List<String> fetch = store_type_data_bkp.get(formatted_string[0]);
            for (String s : fetch) {
                if (s.equals(str)) {
                    fetch.remove(s);
                    break;
                }
            }
            if (fetch.size() == 0) {
                store_type_data_bkp.remove(formatted_string[0]);
            }
        }

        for (Integer it : store_id_OrigData_bkp.keySet()) {
            if (formatted_string[2].equals(store_id_OrigData_bkp.get(it))) {
                tupleIDFound = it;
                break;
            }

        }

        for (int i = 0; i < parsedData.length; i++) {
            if (posting_list_bkp.containsKey(parsedData[i].trim())) {
                List<postingListData> l = posting_list_bkp.get(parsedData[i].trim());
                for (postingListData posting : l) {
                    if (tupleIDFound == posting.term_tuple_id) {
                        l.remove(posting);
                        break;
                    }
                }
                if (l.size() == 0) {
                    posting_list_bkp.remove(parsedData[i].trim());
                }
            }

        }

        store_id_type_bkp.remove(tupleIDFound);
        store_id_OrigData_bkp.remove(tupleIDFound);
        store_id_exactData_bkp.remove(tupleIDFound);

        //System.out.println("removed tuple ID from Backup" + tupleIDFound + " and data " + str);

    }

    public static void removeTupleFromFile(String filePath, String lineToRemove) {

        try {
            File inputFile = new File(filePath);
            File tempFile = new File(inputFile.getParent() + "/temp.txt");

            tempFile.createNewFile();

            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
            String currentLine;
            boolean found = false;

            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                if (trimmedLine.equals(lineToRemove) && (!found)) {
                    found = true;
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            writer.close();
            reader.close();
            boolean successful = tempFile.renameTo(inputFile);

        } catch (FileNotFoundException ex) {
            System.out.println("removeTupleFromFile : failed");
        } catch (IOException ex) {
            System.out.println("removeTupleFromFile : failed");
        }
    }

    public static void removeTupleFromFile_bkp(String filePath, String lineToRemove) {

        try {
            File inputFile = new File(filePath);
            File tempFile = new File(inputFile.getParent() + "/temp.txt");

            tempFile.createNewFile();

            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
            String currentLine;
            boolean found = false;

            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                if (trimmedLine.equals(lineToRemove) && (!found)) {
                    found = true;
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            writer.close();
            reader.close();
            boolean successful = tempFile.renameTo(inputFile);

        } catch (FileNotFoundException ex) {
            System.out.println("removeTupleFromFile_bkp : failed");
        } catch (IOException ex) {
            System.out.println("removeTupleFromFile_bkp : failed");
        }
    }

    public static boolean[] checkSyntaxFormat(String[] parsedInputfinal) {

        boolean[] format = new boolean[2];
        String[] parsedInputExtract = new String[parsedInputfinal.length - 1];
        boolean validSyntax = false;
        boolean validPortFormat = false;

        if (parsedInputfinal.length <= 1) {
            System.out.println("Please enter valid format with proper spaces: add (hostname ip address port) (hostname ip address port) ");
            System.out.println("linda>");
            validSyntax = false;
        } else {
            for (int i = 0; i < parsedInputfinal.length; i++) {
                if (!parsedInputfinal[i].equals("add")) {
                    int countOccurences = parsedInputfinal[i].length() - parsedInputfinal[i].replaceAll("\\(", "").length();
                    if (countOccurences > 1) {
                        System.out.println("Please enter valid format with proper spaces: add (hostname ip address port) (hostname ip address port) ");
                        System.out.println("linda>");
                        validSyntax = false;
                        break;
                    } else {
                        validSyntax = true;
                        for (int k = 0; k < parsedInputExtract.length; k++) {
                            parsedInputExtract[k] = parsedInputfinal[k + 1].substring(parsedInputfinal[k + 1].indexOf("(") + 1, parsedInputfinal[k + 1].indexOf(")"));
                        }

                        for (int p = 0; p < parsedInputExtract.length; p++) {
                            String[] parsedInput = parsedInputExtract[p].split("\\,");
                            if (parsedInput.length == 3 & isPortValid(Integer.parseInt(parsedInput[2]))) {
                                validPortFormat = true;
                            } else {
                                System.out.println("Please enter valid Port Number  > 1024 and  < 65535 ");
                                System.out.println("linda>");
                                validPortFormat = false;
                            }

                        }

                    }
                }

            }

        }
        format[0] = validSyntax;
        format[1] = validPortFormat;
        return format;

    }

    public static void sendBackupTupleData(int recoverHostID) {

        Socket connectionSocket;
        try {
            String other_host_ip = null;
            int other_host_port = -1;
            other_host_ip = connected_host_details.get(recoverHostID).getIp();
            other_host_port = connected_host_details.get(recoverHostID).getPort();

            connectionSocket = new Socket(other_host_ip, other_host_port); //connect with other host
            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
            String line = "";

            BufferedReader br = new BufferedReader(new FileReader(absolutePathBackupTuple));
            try {

                output.println("transferOrigTuple");
                while ((line = br.readLine()) != null) {
                    output.println(line);
                }
                output.println("end");
            } catch (IOException e) {
                System.out.println("sendBackupTupleData : failed");
            } finally {
                br.close();
            }

            boolean close = false;
            int i = 5;
            while (i >= 0) {
                String read_response = in.readLine();
                if (read_response.equals("stored")) {
                    System.out.println("Backup Tuple data transferred to recover host successfully");
                    close = true;
                    break;
                }
                Thread.sleep(1000);
                i--;
            }
            if (!close) {
                System.out.println("tuple on " + other_host_ip + " on port " + other_host_port + " not store properly");
            }
            in.close();
            output.close();
            connectionSocket.close();
        } catch (IOException e) {
            System.out.println("sendBackupTupleData : failed");
        } catch (InterruptedException e) {
            System.out.println("sendBackupTupleData : failed");
        }

    }

    public static void sendMasterInformation(int recoverHostID) {

        Socket connectionSocket;
        try {
            String other_host_ip = connected_host_details.get(recoverHostID).getIp();
            int other_host_port = connected_host_details.get(recoverHostID).getPort();


            connectionSocket = new Socket(other_host_ip, other_host_port); //connect with other host
            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

            output.println("updateMaster " + master_id + " " + master_ip + " " + master_port);

            boolean close = false;
            int i = 5;
            while (i >= 0) {
                String read_response = in.readLine();
                if (read_response.equals("updated")) {
                    close = true;
                    break;
                }
                Thread.sleep(1000);
                i--;
            }
            if (!close) {
                System.out.println("tuple on " + other_host_ip + " on port " + other_host_port + " not store properly");
            }
            in.close();
            output.close();
            connectionSocket.close();
        } catch (IOException e) {
            System.out.println("sendBackupTupleData : OUT message send failed");
        } catch (InterruptedException e) {
            System.out.println("sendBackupTupleData : OUT message send failed");
        }

    }

    public static void sendOrigTupleData(int recoverHostID) {

        Socket connectionSocket;
        try {
            String other_host_ip = null;
            int other_host_port = -1;
            other_host_ip = connected_host_details.get(recoverHostID).getIp();
            other_host_port = connected_host_details.get(recoverHostID).getPort();


            connectionSocket = new Socket(other_host_ip, other_host_port); //connect with other host
            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
            String line = "";
            List<Integer> hostpartitionList = myTopology.getNodeToPartition().get(local_id);

            for(int i= 0;i<hostpartitionList.size();i++){
                String filePath = "/tmp/nshah/linda/"+local_hostname+"/partition_"+hostpartitionList.get(i)+".txt";
                BufferedReader br = new BufferedReader(new FileReader(filePath));
                try {

                    output.println("transferBKPTuple");
                    while ((line = br.readLine()) != null) {
                        output.println(line);
                    }
                    output.println("end");
                } catch (IOException e) {
                    System.out.println("sendOrigTupleData : failed");
                } finally {
                    br.close();
                }


            }


            boolean close = false;
            int i = 5;
            while (i >= 0) {
                String read_response = in.readLine();
                if (read_response.equals("stored")) {
                    //System.out.println("Backup Tuple data transferred to recover host successfully");
                    close = true;
                    break;
                }
                Thread.sleep(5000);
                i--;
            }
            if (!close) {
                System.out.println("tuple on " + other_host_ip + " on port " + other_host_port + " not store properly");
            }
            in.close();
            output.close();
            connectionSocket.close();
        } catch (IOException e) {
            System.out.println("sendOrigTupleData : failed");
        } catch (InterruptedException e) {
            System.out.println("sendOrigTupleData : failed");
        }

    }


    public static void broadcastDeleteRequest(int hostId, String hostName, String hostIP, int hostPort) {
        Socket connectionSocket;

        for (serverDetails serverDetails : connected_host_details.values()) {
            if (serverDetails.getHostID() != local_id) {

                try {
                    connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                    BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);


                    String line = "";
                    output.println("deleteRequest " + hostId + " " + hostName + " " + hostIP + " " + hostPort); //send number of lines are going to transfer.


                    String read_response = in.readLine();
                    while (true) {
                        if (read_response.equals("deleted")) {
                            //System.out.println("host " + hostName + " " + hostIP + " information deleted successfully");
                            in.close();
                            output.close();
                            break;
                        }
                    }

                    in.close();
                    output.close();
                    connectionSocket.close();
                } catch (IOException e) {
                    System.out.println("broadcastDeleteRequest : failed");
                    continue;
                }
            }

        }

    }

    public static void deleteHostInfoFromLocal(int hostId, String hostName, String hostIP, int hostPort) {
        hostname_id_map.remove(hostName);
        hostid_backuphostid.remove(hostId);
        connected_host_details.remove(hostId);
        total_host_in_ds = connected_host_details.size();
        String lineToRemove = hostId + " " + hostName + " " + hostIP + " " + hostPort;
        removeHostInfoFromFile(hostsFilePath, lineToRemove);
    }

    public static boolean deleteCurrentHost(String host_name,Map<Integer, List<Integer>> deletePartition) {
        boolean flg = false;
        try {
            Runtime r = Runtime.getRuntime();
            Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + host_name + "/nets.txt");
            Process changePermissions2 = r.exec("rm -r /tmp/nshah/linda/" + host_name + "/backup_tuples.txt");
            changePermissions1.waitFor();
            changePermissions2.waitFor();

            for(Map.Entry<Integer, List<Integer>> key : deletePartition.entrySet())
            {
                List<Integer> list = key.getValue();
                for(int i =0;i<list.size();i++){
                    Process changePermissions3 = r.exec("rm -r /tmp/nshah/linda/" + host_name + "/partition_"+list.get(i)+".txt");
                    changePermissions3.waitFor();
                }
                flg = true;

            }

            if(!flg){
                String[] b = new String[] {"/bin/sh", "-c", "rm -r /tmp/nshah/linda/" + host_name + "/partition*"};
                Runtime.getRuntime().exec(b);
            }

        } catch (IOException e) {
            System.out.println("deleteCurrentHost : can not delete file. Error occurred " + e.getMessage());
            return false;
        } catch (InterruptedException e1) {
            System.out.println("deleteCurrentHost : can not delete file. Error occurred " + e1.getMessage());
            return false;
        }


        System.out.println("CurrentHost " + host_name  + " deleted successfully");
        return true;
    }

    public static void removeHostInfoFromFile(String filePath, String lineToRemove) {

        try {
            File inputFile = new File(filePath);
            File tempFile = new File(inputFile.getParent() + "/hosttemp.txt");

            tempFile.createNewFile();

            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
            String currentLine;
            boolean found = false;

            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                if (trimmedLine.equals(lineToRemove) && (!found)) {
                    found = true;
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            writer.close();
            reader.close();
            boolean successful = tempFile.renameTo(inputFile);

        } catch (FileNotFoundException ex) {
            System.out.println("removeHostInfoFromFile : failed");
        } catch (IOException ex) {
            System.out.println("removeHostInfoFromFile : failed");
        }
    }

    public static void broadcastRecoveryInfo(String hostsFilePath,String absolutePathTuple,String absolutePathBackupTuple){
        Socket connectionSocket;
        try {
            Runtime r = Runtime.getRuntime();
            Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + local_hostname + "/backup_tuples.txt");
            Process changePermissions2 = r.exec("touch /tmp/nshah/linda/" + local_hostname + "/backup_tuples.txt");
            Process changePermissions3 = r.exec("chmod 777 /tmp/nshah/linda/" + local_hostname +"/backup_tuples.txt");
            changePermissions1.waitFor();
            changePermissions2.waitFor();
            changePermissions3.waitFor();
        } catch (IOException e) {
            System.out.println("broadcastRecoveryInfo : failed");
        } catch (InterruptedException e) {
            System.out.println("broadcastRecoveryInfo : failed");
        }

        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(hostsFilePath));
            while ((line = br.readLine()) != null) {
                String[] split_line = line.trim().split("\\s+");
                serverDetails serverDtls = new serverDetails(Integer.parseInt(split_line[0]),split_line[1],split_line[2],Integer.parseInt(split_line[3]));
                connected_host_details.put(Integer.parseInt(split_line[0]),serverDtls);
                hostname_id_map.put(split_line[1],Integer.parseInt(split_line[0]));
                if(split_line[1].equals(local_hostname) && split_line[2].equals(local_ip)){
                    local_id = Integer.parseInt(split_line[0]);
                    myServer.setHostID(local_id);
                }
                myTopology.addNewHost(serverDtls);
                //myTopology.printNodeToPartitions();
                //myTopology.printPartitionsToNode();
                //System.out.println();

            }
            br.close();
            updateHostInfoFile(hostsFilePath,local_hostname,local_ip,local_port);
            serverDetails getDtls = connected_host_details.get(local_id);
            serverDetails updateDtls = new serverDetails(getDtls.getHostID(),getDtls.getHostName(),getDtls.getIp(),local_port);
            connected_host_details.put(local_id,updateDtls);
            createBackupHostLkp();

            List<Integer> hostPartition = myTopology.getNodeToPartition().get(local_id);
            for(int i=0;i<hostPartition.size();i++) {
                try {
                    Runtime r = Runtime.getRuntime();
                    Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + local_hostname + "/partition_"+hostPartition.get(i)+".txt");
                    Process changePermissions2 = r.exec("touch /tmp/nshah/linda/" + local_hostname + "/partition_"+hostPartition.get(i)+".txt");
                    Process changePermissions3 = r.exec("chmod 777 /tmp/nshah/linda/" + local_hostname + "/partition_"+hostPartition.get(i)+".txt");
                    changePermissions1.waitFor();
                    changePermissions2.waitFor();
                    changePermissions3.waitFor();
                } catch (IOException e) {
                    System.out.println("broadcastRecoveryInfo : failed");
                    continue;
                } catch (InterruptedException e) {
                    System.out.println("broadcastRecoveryInfo : failed");
                    continue;
                }
            }


            for (serverDetails serverDetails : connected_host_details.values()) {
                if(serverDetails.getHostID()!=local_id)
                {
                    try {
                        connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                        BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                        PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                        output.println("hostRecoveryInfo " + local_hostname + " " + local_ip + " " + local_port);
                        String read_response = in.readLine();
                        if (read_response.equals("updated")) {
                            System.out.println("Recovery Information to " + serverDetails.getIp() + " on port " + serverDetails.getPort() + " sent successfully");
                        }

                        in.close();
                        output.close();
                        connectionSocket.close();
                    } catch (IOException e) {
                        System.out.println("broadcastRecoveryInfo : connection failed");
                        continue;
                    }
                }
            }

            Thread.sleep(500);
            List<Integer> parts = myTopology.getPartitionToNode();
            for(int i=0;i<parts.size();i++) {
                if(parts.get(i)!=local_id) {
                    try {
                        Runtime r = Runtime.getRuntime();
                        Process changePermissions1 = r.exec("rm -r /tmp/nshah/linda/" + local_hostname + "/partition_" + i + ".txt");
                        changePermissions1.waitFor();
                    } catch (IOException e) {
                        System.out.println("broadcastRecoveryInfo : failed");
                        continue;
                    } catch (InterruptedException e) {
                        System.out.println("broadcastRecoveryInfo : failed");
                        continue;
                    }
                }
            }
            myTopology.printNodeToPartitions();
            myTopology.printPartitionsToNode();
            System.out.println();
            System.out.println("recovery host "+getDtls.getHostID()+" "+getDtls.getHostName()+" "+getDtls.getIp()+" "+local_port+ " details updated successfully");

        } catch (Exception e) {
            System.out.println("broadcastRecoveryInfo : failed");
        }



    }

    public static void updateHostInfoFile(String filePath,String hostName,String hostIP,int hostPort) {
        try {
            // Open the file that is the first
            // command line parameter
            FileInputStream fstream = new FileInputStream(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            StringBuilder fileContent = new StringBuilder();
            //Read File Line By Line
            while ((strLine = br.readLine()) != null) {
                // Print the content on the console
                String tokens[] = strLine.split(" ");
                if (tokens.length > 0) {
                    // Here tokens[0] will have value of ID
                    if (tokens[1].trim().equals(hostName)) {
                        tokens[2] = hostIP.trim();
                        tokens[3] = String.valueOf(hostPort);
                        String newLine = tokens[0] + " " + tokens[1] + " " + tokens[2] + " " + tokens[3];
                        fileContent.append(newLine);
                        fileContent.append("\n");
                    } else {
                        // update content as it is
                        fileContent.append(strLine);
                        fileContent.append("\n");
                    }
                }
            }
            // Now fileContent will have updated content , which you can override into file
            FileWriter fstreamWrite = new FileWriter(filePath);
            BufferedWriter out = new BufferedWriter(fstreamWrite);
            out.write(fileContent.toString());
            out.close();
            //Close the input stream
            fstream.close();
        } catch (Exception e) {//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Please enter hostname");
            System.out.println("linda>");
            return;
        }

        String s2 = args[0];

        hostsFilePath = createDirectory(s2);
        absolutePathTuple = "/tmp/nshah/linda/"+ s2;
        absolutePathBackupTuple = createBackupTupleDirectory(s2);
        boolean alreadyExist = false;

        if (hostsFilePath.equalsIgnoreCase("NotSuccess") | absolutePathBackupTuple.equalsIgnoreCase("NotSuccess")) {
            alreadyExist = true;
        }

        /*Change Directory Permission*/
        if(!alreadyExist)
        {   Runtime r = Runtime.getRuntime();
            Process changePermissions1 = r.exec("chmod 777 /tmp/nshah");
            Process changePermissions2 = r.exec("chmod 777 /tmp/nshah/linda");
            Process changePermissions3 = r.exec("chmod 777 /tmp/nshah/linda/" + s2);
            Process changePermissions5 = r.exec("chmod 666 /tmp/nshah/linda/" + s2 + "/nets.txt");
            Process changePermissions6 = r.exec("chmod 666 /tmp/nshah/linda/" + s2 + "/backup_tuples.txt");
            changePermissions1.waitFor();
            changePermissions2.waitFor();
            changePermissions3.waitFor();
            changePermissions5.waitFor();
            changePermissions6.waitFor();
        }
        else {
            absolutePathTuple = "/tmp/nshah/linda/" + s2;
            hostsFilePath = "/tmp/nshah/linda/"+ s2 + "/nets.txt";
            absolutePathBackupTuple = "/tmp/nshah/linda/" + s2 + "/backup_tuples.txt";
        }

        tupleSocket = new ServerSocket(0);
        int port = tupleSocket.getLocalPort();
        String local_ipadd = getIP();

        local_port = port;
        local_ip = local_ipadd;
        local_hostname = s2;

        myServer = new serverDetails(local_hostname, local_ip, local_port);
        myTopology = new TopologyInfo(myServer);
        System.out.println(local_ip + " at port number: " + port);
        System.out.print("linda> ");

        /*Client Thread started to read request from command propmp and to communicate with other hosts*/
        commandPromptClient cmdClient = null;
        try {
            cmdClient = new commandPromptClient(hostsFilePath, alreadyExist);
            cmdClient.start();
        }
        catch (Exception e){
            System.out.println("Main method : commandPromptClient Error");
            e.printStackTrace();
        }
        try {
            while (true && !nodeDeleted) {
                ServerListener w;
                try {
                    w = new ServerListener(tupleSocket.accept(), hostsFilePath, tupleSocket); //Start client accepter thread
                    Thread t = new Thread(w);
                    t.start();
                } catch (Exception e) {
                    if(!nodeDeleted) {
                        System.out.println("Server Listener Error in connection");
                    }
                }
            }
        } finally {
            if(tupleSocket!=null) {
                tupleSocket.close();
            }
        }

        // if command prompt thread is not closed then close it
        //System.out.println("cmdClient != null"+(cmdClient != null)+ "nodeDeleted "+nodeDeleted);
        if (cmdClient != null && nodeDeleted) {
            cmdClient.shutdown();
        }
        System.out.println("Exiting the entire system");
        System.exit(0);
    }


    /*This class servers as a client and it will serve all the requests coming from the user through command prompt*/
    private static class commandPromptClient extends Thread {

        private Socket connectionSocket;
        private String hostsFilePath;
        private boolean alreadyExist;
        private BufferedReader reader;
        private boolean cmdPromptShutdown = false;

        public commandPromptClient(String filePath,boolean alreadyExist) {
            this.hostsFilePath = filePath;
            this.alreadyExist = alreadyExist;
        }

        public void shutdown() {
            cmdPromptShutdown = true;
            try {
                if (connectionSocket != null) {
                    connectionSocket.close();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception e) {
                System.out.println("Issues while closing command prompt socket. Error: " + e.getMessage());
            }
        }

        public void run() {
            if(alreadyExist){
                broadcastRecoveryInfo(hostsFilePath,absolutePathTuple,absolutePathBackupTuple);
                System.out.println("linda>");
            }
            while (true && !cmdPromptShutdown) {
                try {

                    reader = new BufferedReader(new InputStreamReader(System.in));
                    String input = null;
                    if (reader.ready()) {
                        input = reader.readLine(); //"add host_2 127.0.0.1 1342"
                    } else {
                        this.sleep(500);
                        continue;
                    }
                    if (input.trim().equals("")) {
                        continue;
                    } else if (input.trim().equals("quit")) {
                        break;
                    }
                    if (input.startsWith("add") && (master_ip == null)) {
                        String[] parsedInputfinal = input.split("\\s+");
                        String[] parsedInputExtract = new String[parsedInputfinal.length - 1];
                        boolean[] format = new boolean[2];
                        boolean validSyntax = false;
                        boolean validPortFormat = false;

                        format = checkSyntaxFormat(parsedInputfinal);
                        validSyntax = format[0];
                        validPortFormat = format[1];

                        /*Add after validating syntax and format*/
                        if (validSyntax && validPortFormat) {
                            for (int i = 0; i < parsedInputExtract.length; i++) {
                                parsedInputExtract[i] = parsedInputfinal[i + 1].substring(parsedInputfinal[i + 1].indexOf("(") + 1, parsedInputfinal[i + 1].indexOf(")"));
                            }

                            boolean isValidformat = false;

                            //block to add all host in direcotry at local
                            for (int i = 0; i < parsedInputExtract.length; i++) {
                                String[] parsedInput = parsedInputExtract[i].split("\\,");

                                if ((parsedInputfinal[0].equals("add")) & parsedInput.length == 3 & isPortValid(Integer.parseInt(parsedInput[2]))) {
                                    if (master_ip == null) {
                                        Integer host_id_gererate = (connected_host_details.size() ==0 ? 0 :Collections.max(connected_host_details.keySet()));
                                        while (connected_host_details.containsKey(host_id_gererate)) {
                                            host_id_gererate++;
                                        }
                                        master_ip = local_ip;
                                        master_port = local_port;
                                        master_id = host_id_gererate;
                                        local_id = host_id_gererate;
                                        host_id_gererate = host_id_gererate;
                                        myServer.setHostID(local_id); // Set the master id into this server
                                        myTopology.recalculatePartitions(myServer);
                                        System.out.println();
                                        System.out.println("After adding host : ID "+local_id+" Name "+local_hostname+" IP "+local_ip+" Port "+local_port);
                                        myTopology.printNodeToPartitions();
                                        myTopology.printPartitionsToNode();
                                        System.out.println();
                                        connected_host_details.put(local_id, myServer);
                                        addHostDetails(local_id, local_hostname, local_ip, local_port, hostsFilePath); // entry in /net directory
                                        hostname_id_map.put(local_hostname, local_id);
                                    }

                                    Integer host_id_gererate = (connected_host_details.size() ==0 ? 0 :Collections.max(connected_host_details.keySet()));
                                    while (connected_host_details.containsKey(host_id_gererate)) {
                                        host_id_gererate++;
                                    }

                                    int other_host_id = host_id_gererate;
                                    host_id_gererate = host_id_gererate;

                                    serverDetails otherServer = new serverDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]));
                                    myTopology.addNewHost(otherServer);
                                    System.out.println();
                                    System.out.println("After adding host : ID "+otherServer.getHostID()+" Name "+otherServer.getHostName()+" IP "+otherServer.getIp()+" Port "+otherServer.getPort());
                                    myTopology.printNodeToPartitions();
                                    myTopology.printPartitionsToNode();
                                    System.out.println();

                                    connected_host_details.put(other_host_id,otherServer);
                                    addHostDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]), hostsFilePath);
                                    hostname_id_map.put(parsedInput[0], other_host_id);
                                    createBackupHostLkp();
                                    isValidformat = true;


                                } else {
                                    System.out.println("Invalid format or port number");
                                    System.out.println("Please enter valid format: add (hostname ip address port) (hostname ip address port) ");
                                    System.out.println("Please enter valid Port Number  > 1024 and  < 65535 ");
                                    isValidformat = false;
                                    break;
                                }


                            }

                            // Block to inform all hosts to add host information into their local machine
                            if (isValidformat) {
                                total_host_in_ds = connected_host_details.size(); //Strore number of hosts in system

                                for (serverDetails serverDetails : connected_host_details.values()) {
                                    try {

                                        if (serverDetails.getHostID() != local_id) {
                                            connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                            String line = "";
                                            BufferedReader br = new BufferedReader(new FileReader(hostsFilePath));
                                            try {
                                                output.println("add hosts"); //send number of lines are going to transfer.
                                                while ((line = br.readLine()) != null) {
                                                    output.println(line);
                                                }
                                                output.println("end");
                                            } catch (IOException e) {
                                                System.out.println("serverDetails : connection failed");
                                                //e.printStackTrace();
                                            } finally {
                                                br.close();
                                            }

                                            boolean close = false;
                                            int i = 5;
                                            while (i >= 0) {
                                                String read_response = in.readLine();
                                                if (read_response.equals("added")) {
                                                    System.out.println("Connection with " + serverDetails.getIp() + " on port " + serverDetails.getPort() + " established successfully");
                                                    close = true;
                                                    break;
                                                }
                                                Thread.sleep(1000);
                                                i--;
                                            }
                                            if (!close) {
                                                System.out.println("Connection with " + serverDetails.getIp() + " on port " + serverDetails.getPort() + " unsuccessful");
                                            }
                                            in.close();
                                            output.close();
                                            connectionSocket.close();
                                        }
                                    } catch (IOException e) {
                                        System.out.println("connection with host failed");
                                        continue;
                                    }
                                }
                            }

                        }

                        createTupleDirectory(local_hostname);
                        System.out.println("partition wise tuple files created");
                        System.out.println("Master of this host : ID "+master_id+" IP "+master_ip+" Port "+master_port);
                        System.out.println("linda>");

                        //If add new host after stable DS formed
                    } else if (input.startsWith("add") && (master_ip != null)) {
                        String[] parsedInputfinal = input.split("\\s+");
                        String[] parsedInputExtract = new String[parsedInputfinal.length - 1];
                        boolean[] format = new boolean[2];
                        boolean validSyntax = false;
                        boolean validPortFormat = false;

                        format = checkSyntaxFormat(parsedInputfinal);
                        validSyntax = format[0];
                        validPortFormat = format[1];

                        for (int i = 0; i < parsedInputExtract.length; i++) {
                            parsedInputExtract[i] = parsedInputfinal[i + 1].substring(parsedInputfinal[i + 1].indexOf("(") + 1, parsedInputfinal[i + 1].indexOf(")"));
                        }

                        /*Add after validating syntax and format*/

                        if (validSyntax && validPortFormat && (master_ip.equals(local_ip)) && (master_port == local_port)) {

                            List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();


                            //block to add all host stored in direcotry at local and also add new host in local
                            for (int i = 0; i < parsedInputExtract.length; i++) {
                                String[] parsedInput = parsedInputExtract[i].split("\\,");

                                Integer host_id_gererate = (connected_host_details.size() ==0 ? 0 :Collections.max(connected_host_details.keySet()));
                                while (connected_host_details.containsKey(host_id_gererate)) {
                                    host_id_gererate++;
                                }

                                int other_host_id = host_id_gererate;
                                host_id_gererate = host_id_gererate;

                                serverDetails otherServer = new serverDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]));
                                List<Integer> partitionsToBeCopied = myTopology.addNewHost(otherServer);
                                totalpartitionListToTransfer.add(partitionsToBeCopied);
                                System.out.println();
                                System.out.println("After adding host : ID "+otherServer.getHostID()+" Name "+otherServer.getHostName()+" IP "+otherServer.getIp()+" Port "+otherServer.getPort());
                                myTopology.printNodeToPartitions();
                                myTopology.printPartitionsToNode();
                                System.out.println();

                                connected_host_details.put(other_host_id,otherServer);
                                addHostDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]), hostsFilePath);
                                hostname_id_map.put(parsedInput[0], other_host_id);



                                for (serverDetails serverDetails : connected_host_details.values()) {
                                    try {
                                        if ((serverDetails.getHostID() != local_id) && (serverDetails.getHostID() != other_host_id)) {
                                            connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                            output.println("addNew " + other_host_id + " " + parsedInput[0] + " " + parsedInput[1] + " " + Integer.parseInt(parsedInput[2])); //send number of lines are going to transfer.


                                            String read_response = in.readLine();
                                            if (read_response.equals("added")) {
                                                in.close();
                                                output.close();
                                            }

                                            in.close();
                                            output.close();
                                            connectionSocket.close();
                                        } else if ((serverDetails.getHostID() != local_id) && (serverDetails.getHostID() == other_host_id)) {
                                            connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                            String line = "";
                                            BufferedReader br = new BufferedReader(new FileReader(hostsFilePath));
                                            try {
                                                output.println("add hosts"); //send number of lines are going to transfer.
                                                while ((line = br.readLine()) != null) {
                                                    output.println(line);
                                                }
                                                output.println("end");
                                            } catch (IOException e) {
                                                System.out.println("addNew : connection failed");
                                                //e.printStackTrace();
                                            } finally {
                                                br.close();
                                            }

                                            boolean close = false;
                                            int k = 5;
                                            while (k >= 0) {
                                                String read_response = in.readLine();
                                                //System.out.println("response " + read_response);
                                                if (read_response.equals("added")) {
                                                    close = true;
                                                    break;
                                                }
                                                Thread.sleep(1000);
                                                k--;
                                            }
                                            if (!close) {
                                                System.out.println("Connection with " + serverDetails.getIp() + " on port " + serverDetails.getPort() + " unsuccessful");
                                            }

                                            transferTopologyInformation(serverDetails.getIp(),serverDetails.getPort());
                                            transferPartitionToNewHost(partitionsToBeCopied,serverDetails.getIp(),serverDetails.getPort());
                                            System.out.println("Partition information and data transferred to new host successfully");

                                            in.close();
                                            output.close();
                                            connectionSocket.close();

                                        }
                                    } catch (IOException e) {
                                        System.out.println("New host connection details failed");
                                        continue;
                                    }

                                }

                                Thread.sleep(1000);

                            }

                            total_host_in_ds = connected_host_details.size(); //Strore number of hosts in system
                            boolean backupHostChange = createBackupHostLkp();
                            //To DO : handle is servedAsBackup changed
                            if(backupHostChange){
                                TransferOriginalTupleData(totalpartitionListToTransfer);
                            }else {
                                removePartitionDataFromBackupHost(totalpartitionListToTransfer);
                            }

                            System.out.println("New hosts information added successfully");
                            System.out.println("total hosts in system " + total_host_in_ds);
                            System.out.println("linda>");

                        } else if (validSyntax && validPortFormat && (master_port != local_port)/*(!master_ip.equals(local_ip))*/) {
                            String get_master_ip = master_ip;
                            int get_master_port = master_port;
                            boolean success = false;
                            System.out.println("Master of this host " + get_master_ip + " " + get_master_port);
                            List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();

                            for (int i = 0; i < parsedInputExtract.length; i++) {
                                String[] parsedInput = parsedInputExtract[i].split("\\,");

                                try {
                                    connectionSocket = new Socket(get_master_ip, get_master_port); //connect with other host
                                    BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
                                    String line = "";
                                    output.println("newRequest " + parsedInput[0] + " " + parsedInput[1] + " " + Integer.parseInt(parsedInput[2]));

                                    while (true) {
                                        String read_response = in.readLine();
                                        if (read_response.equals("newRequestServed")) {
                                            System.out.println("New request has been forwarded to master host");
                                            success = true;
                                            break;
                                        }
                                    }

                                    in.close();
                                    output.close();
                                    connectionSocket.close();

                                } catch (IOException e) {
                                    //e.printStackTrace();
                                    System.out.println("Master host does not responding : server has been crashed");
                                    success = false;
                                    break;
                                }

                                Thread.sleep(1000);

                            }

                        /*Master server either deleted or crashed hence new host should be elected to be master*/
                            if (!success) {

                                System.out.println("Start to elect new master");
                                String max_ip = local_ip;
                                int max_port = local_port;
                                int max_id = local_id;

                                for (serverDetails serverDetails : connected_host_details.values()) {
                                    try {
                                        if (serverDetails.getHostID() != local_id) {
                                            System.out.println("take vote to Elect new master from " + serverDetails.getIp() + " " + serverDetails.getPort());
                                            connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                            output.println("elect " + max_id + " " + max_ip + " " + max_port); //send number of lines are going to transfer.


                                            String read_response = in.readLine();
                                            String[] split_read_response = read_response.split("\\s+");
                                            max_id = Integer.parseInt(split_read_response[0]);
                                            max_ip = split_read_response[1];
                                            max_port = Integer.parseInt(split_read_response[2]);


                                            in.close();
                                            output.close();
                                            connectionSocket.close();
                                        }

                                    } catch (IOException e) {
                                        //e.printStackTrace();
                                        System.out.println("host connection failed");
                                        continue;
                                    }

                                }

                                master_ip = max_ip;
                                master_port = max_port;
                                master_id = max_id;
                                System.out.println("New elected master : " + master_id + " " + master_ip + " " + master_port);

                            /*If new elected master server is not local server*/
                                if (!(max_ip.equals(local_ip) && (max_port == local_port))) {
                                    System.out.println("New master server is not local host");
                                    try {
                                        connectionSocket = new Socket(master_ip, master_port); //connect with other host
                                        BufferedReader in1 = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                        PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
                                        String line = "";
                                        output.println("becomeMaster " + input); //request new master to serve ADD request
                                        in1.close();
                                        output.close();
                                        connectionSocket.close();
                                    } catch (IOException e) {
                                        System.out.println("Can not establish connection with new elected Master ");
                                    }
                                } /*If new elected master server is local server*/
                                else {

                                    System.out.println("New master server is local host");
                                    master_ip = local_ip;
                                    master_port = local_port;
                                    master_id = local_id;

                                    for (int i = 0; i < parsedInputExtract.length; i++) {
                                        String[] parsedInput = parsedInputExtract[i].split("\\,");

                                        Integer host_id_gererate = (connected_host_details.size() ==0 ? 0 :Collections.max(connected_host_details.keySet()));
                                        System.out.println("Collection max Keyset value "+host_id_gererate);
                                        while (connected_host_details.containsKey(host_id_gererate)) {
                                            host_id_gererate++;
                                        }

                                        int other_host_id = host_id_gererate;

                                        serverDetails otherServer = new serverDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]));
                                        List<Integer> partitionsToBeCopied=myTopology.addNewHost(otherServer);
                                        totalpartitionListToTransfer.add(partitionsToBeCopied);
                                        System.out.println("After adding host : ID "+otherServer.getHostID()+" Name "+otherServer.getHostName()+" IP "+otherServer.getIp()+" Port "+otherServer.getPort());
                                        myTopology.printNodeToPartitions();
                                        myTopology.printPartitionsToNode();
                                        System.out.println();

                                        connected_host_details.put(other_host_id,otherServer);
                                        addHostDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]), hostsFilePath);
                                        hostname_id_map.put(parsedInput[0], other_host_id);


                                        for (serverDetails serverDetails : connected_host_details.values()) {
                                            try {
                                                if (serverDetails.getHostID() != local_id && (serverDetails.getHostID() != other_host_id)) {
                                                    connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                                    BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                                    output.println("addNew " + other_host_id + " " + parsedInput[0] + " " + parsedInput[1] + " " + Integer.parseInt(parsedInput[2])); //send number of lines are going to transfer.
                                                    output.flush();

                                                    String read_response = in.readLine();
                                                    if (read_response.equals("added")) {
                                                        in.close();
                                                        output.close();
                                                    }

                                                    in.close();
                                                    output.close();
                                                    connectionSocket.close();
                                                } else if ((serverDetails.getHostID() != local_id) && (serverDetails.getHostID() == other_host_id)) {
                                                    connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                                    BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                                    String line = "";
                                                    BufferedReader br = new BufferedReader(new FileReader(hostsFilePath));
                                                    try {
                                                        output.println("add hosts"); //send number of lines are going to transfer.
                                                        while ((line = br.readLine()) != null) {
                                                            output.println(line);
                                                        }
                                                        output.println("end");
                                                    } catch (IOException e) {
                                                        System.out.println("addNew : connection failed");
                                                        //e.printStackTrace();
                                                    } finally {
                                                        br.close();
                                                    }

                                                    boolean close = false;
                                                    int k = 5;
                                                    while (k >= 0) {
                                                        String read_response = in.readLine();
                                                        //System.out.println("response " + read_response);
                                                        if (read_response.equals("added")) {
                                                            close = true;
                                                            break;
                                                        }
                                                        Thread.sleep(1000);
                                                        k--;
                                                    }
                                                    if (!close) {
                                                        System.out.println("Connection with " + serverDetails.getIp() + " on port " + serverDetails.getPort() + " unsuccessful");
                                                    }

                                                    transferTopologyInformation(serverDetails.getIp(),serverDetails.getPort());
                                                    transferPartitionToNewHost(partitionsToBeCopied,serverDetails.getIp(),serverDetails.getPort());
                                                    System.out.println("Partition data and information transferred to new host successfully");

                                                    in.close();
                                                    output.close();
                                                    connectionSocket.close();

                                                }
                                            } catch (IOException e) {
                                                System.out.println("Added New : host connection details failed");
                                                continue;
                                            }

                                        }

                                        System.out.println("New host "+ other_host_id + " " + parsedInput[0] + " " + parsedInput[1] + " " + Integer.parseInt(parsedInput[2])+" information added successfuly");

                                    }


                                    for (serverDetails serverDetails : connected_host_details.values()) {
                                        try {
                                            if (serverDetails.getHostID() != local_id) {
                                                connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                                BufferedReader in_rq = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                                PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                                output.println("updateMaster " + master_id + " " + master_ip + " " + master_port);


                                                String read_response = in_rq.readLine();
                                                if (read_response.equals("updated")) {
                                                    //String response_2 = in.readLine();
                                                    //System.out.println("response " + response_2);
                                                    in_rq.close();
                                                    output.close();
                                                }

                                                in_rq.close();
                                                output.close();
                                                connectionSocket.close();
                                            }
                                        } catch (IOException e) {
                                            //e.printStackTrace();
                                            System.out.println("Added New host connection details failed");
                                            continue;
                                        }

                                    }

                                    System.out.println("New master information sent to all host");
                                    total_host_in_ds = connected_host_details.size(); //Strore number of hosts in system
                                    System.out.println("total host in system " + total_host_in_ds);
                                    boolean backupHostChange = createBackupHostLkp();
                                    if(backupHostChange){
                                        TransferOriginalTupleData(totalpartitionListToTransfer);
                                    }
                                    else {
                                        removePartitionDataFromBackupHost(totalpartitionListToTransfer);
                                    }
                                }


                            }

                        }

                        System.out.println("linda>");
                    }
                    //If add new end
                    else if (input.startsWith("out")) {
                        String[] formatted_string = new String[3];
                        formatted_string = formatString(input, -1); //required only concate string
                        int keyNumber = getMD5(formatted_string[1]);
                        int find_partition_id = (keyNumber/myTopology.range);
                        int findOrigHost =  myTopology.getPartitionToNode().get(find_partition_id);
                        int findBkpHost = hostid_backuphostid.get(findOrigHost);

                        if (findOrigHost != local_id) {

                            for (int j = 0; j <= 1; j++) {
                                try {
                                    String other_host_ip = null;
                                    int other_host_port = -1;
                                    if (j == 0) {
                                        other_host_ip = connected_host_details.get(findOrigHost).getIp();
                                        other_host_port = connected_host_details.get(findOrigHost).getPort();
                                    } else if (j == 1) {

                                        other_host_ip = connected_host_details.get(findBkpHost).getIp();
                                        other_host_port = connected_host_details.get(findBkpHost).getPort();
                                    }


                                    connectionSocket = new Socket(other_host_ip, other_host_port); //connect with other host
                                    BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
                                    String line = "";

                                    if (j == 0) {
                                        output.println("store " + input);
                                    } else if (j == 1) {
                                        output.println("BackupStore " + input);
                                    }

                                    boolean close = false;
                                    int i = 5;
                                    while (i >= 0) {
                                        String read_response = in.readLine();
                                        //System.out.println("response " + read_response);
                                        if (read_response.equals("stored")) {
                                            if(j==0) {
                                                System.out.println("put tuple (" + formatted_string[2] + ") on " + other_host_ip + " on port " + other_host_port + " on partition " + find_partition_id);
                                            }
                                            else if(j==1){
                                                System.out.println("put tuple (" + formatted_string[2] + ") on backup host " + other_host_ip + " on port " + other_host_port + " on partition " + find_partition_id);
                                            }
                                            close = true;
                                            break;
                                        }
                                        Thread.sleep(1000);
                                        i--;
                                    }
                                    if (!close) {
                                        System.out.println("tuple on " + other_host_ip + " on port " + other_host_port + " not store properly");
                                    }
                                    in.close();
                                    output.close();
                                    connectionSocket.close();
                                } catch (IOException e) {
                                    //e.printStackTrace();
                                    System.out.println("connection with host : failed");
                                    continue;
                                }
                            }


                        } else {

                            while (store_id_OrigData.containsKey(tuple_id)){
                                tuple_id++;
                            }
                            String[] formatted_data = new String[3];
                            formatted_data = formatString(input, tuple_id);
                            String temp = "(" + formatted_data[2] + ")";
                            String partitionFilepath = absolutePathTuple+"/partition_"+find_partition_id+".txt";
                            addtuples(temp, partitionFilepath);
                            System.out.println("put tuple (" + formatted_data[2] + ") on " + local_ip + " on " + local_port+ " on partition "+find_partition_id);

                            String other_host_ip = connected_host_details.get(findBkpHost).getIp();
                            int other_host_port = connected_host_details.get(findBkpHost).getPort();


                            //System.out.println("sending OUT message to Backup host IP " + other_host_ip + " port" + other_host_port);

                            try {
                                connectionSocket = new Socket(other_host_ip, other_host_port); //connect with other host
                                BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
                                String line = "";

                                output.println("BackupStore " + input);

                                boolean close = false;
                                int i = 5;
                                while (i >= 0) {
                                    String read_response = in.readLine();
                                    if (read_response.equals("stored")) {
                                        System.out.println("put tuple (" + formatted_string[2] + ") on backup host " + other_host_ip + " on port " + other_host_port);
                                        close = true;
                                        break;
                                    }
                                    Thread.sleep(1000);
                                    i--;
                                }
                                if (!close) {
                                    System.out.println("tuple on " + other_host_ip + " on port " + other_host_port + " not store properly");
                                }
                                in.close();
                                output.close();
                                connectionSocket.close();
                            } catch (IOException e) {
                                System.out.println("OUT message to backup host failed");
                            }
                        }

                        System.out.println("linda>");


                    } //if out end
                    else if (input.startsWith("delete")) {

                        String extractString = input.substring(input.indexOf("(") + 1, input.indexOf(")"));
                        String[] split_string = extractString.split(",");
                        boolean currentHostDeleteReqReceived = false;
                        boolean success = false;
                        boolean bkpHostChange = false;
                        boolean deleteSuccessful = false;
                        Map<Integer, List<Integer>> partitionsToBeDistributedMap = new HashMap<Integer,List<Integer>>();
                        for (int it = 0; it < split_string.length; it++) {


                            if (hostname_id_map.containsKey(split_string[it])) {
                                //System.out.println("delete host ID " + hostname_id_map.get(split_string[it]));
                                serverDetails serverDtls = connected_host_details.get(hostname_id_map.get(split_string[it].trim()));

                                if (serverDtls.getHostID() != local_id) {
                                    partitionsToBeDistributedMap = myTopology.deleteHost(serverDtls);
                                    myTopology.printNodeToPartitions();
                                    myTopology.printPartitionsToNode();
                                    System.out.println("delete host request is not local host ");
                                    broadcastDeleteRequest(serverDtls.getHostID(), serverDtls.getHostName(), serverDtls.getIp(), serverDtls.getPort());
                                    deleteHostInfoFromLocal(serverDtls.getHostID(), serverDtls.getHostName(), serverDtls.getIp(), serverDtls.getPort());
                                    //System.out.println("deleteHostInfoFromLocal : host information deleted successfully");
                                    if(!bkpHostChange){
                                        bkpHostChange = createBackupHostLkp();
                                    }
                                } else {
                                    //broadcastDeleteRequest(serverDtls.getHostID(), serverDtls.getHostName(), serverDtls.getIp(), serverDtls.getPort());
                                    currentHostDeleteReqReceived = true;
                                    continue;

                                }

                            } else {
                                System.out.println("Host Name which you have entered does not exists. Please choose valid host Name");
                            }

                            createBackupHostLkp();
                            //System.out.println("After broadcast Delete :backup host of local bkpHostChange :" + bkpHostChange);





                        /*if (currentHostDeleteReqReceived) {

                            currentHostDeleteReqReceived = true;
                            serverDetails serverDtls = connected_host_details.get(hostname_id_map.get(local_hostname));
                            //broadcastDeleteRequest(serverDtls.getHostID(), serverDtls.getHostName(), serverDtls.getIp(), serverDtls.getPort());

                            System.out.println("delete host request is local host and ID "+serverDtls.getHostID());
                            createBackupHostLkp();
                            myTopology.printPartitionsToNode();
                            myTopology.printNodeToPartitions();
                            for (Map.Entry<Integer, List<Integer>> nTp : partitionsToBeDistributedMap.entrySet()) {
                                int hostID = nTp.getKey();
                                List<Integer> partitionList = partitionsToBeDistributedMap.get(hostID);
                                String hostIP = connected_host_details.get(hostID).getIp();
                                int hostPort = connected_host_details.get(hostID).getPort();
                                transferPartitionToNewHost(partitionList,hostIP,hostPort);

                            }
                            deleteSuccessful = deleteCurrentHost(local_hostname,partitionsToBeDistributedMap);

                        }
                        else {*/

                            if (bkpHostChange) {
                                //System.out.println("local host backup host before : " + localhost_bkpHostID);
                                Thread.sleep(1000);
                                List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();
                                TransferOriginalTupleData(totalpartitionListToTransfer);
                                localhost_bkpHostID = hostid_backuphostid.get(local_id);
                                //System.out.println("local host backup host after : " + localhost_bkpHostID);
                            }

                            createTupleDirectory(local_hostname);
                            //}

                            //Thread.sleep(1000);
                        }

                        if (currentHostDeleteReqReceived) {
                            Thread.sleep(1000);
                            serverDetails serverDtls = connected_host_details.get(hostname_id_map.get(local_hostname));
                            System.out.println("delete host request is local host and ID "+serverDtls.getHostID());
                            partitionsToBeDistributedMap = myTopology.deleteHost(serverDtls);
                            myTopology.printNodeToPartitions();
                            myTopology.printPartitionsToNode();

                            //System.out.println( "ID from topology "+myTopology.getNodeToPartition().get(0)+"list" + myTopology.getNodeToPartition().get(local_id));
                            //Map<Integer, List<Integer>> partitionsToBeDistributedMap = myTopology.deleteHost(serverDtls);
                            myTopology.printPartitionsToNode();
                            myTopology.printNodeToPartitions();
                            for (Map.Entry<Integer, List<Integer>> nTp : partitionsToBeDistributedMap.entrySet()) {
                                int hostID = nTp.getKey();
                                List<Integer> partitionList = partitionsToBeDistributedMap.get(hostID);
                                String hostIP = connected_host_details.get(hostID).getIp();
                                int hostPort = connected_host_details.get(hostID).getPort();
                                transferPartitionToNewHost(partitionList,hostIP,hostPort);

                            }

                            broadcastDeleteRequest(serverDtls.getHostID(), serverDtls.getHostName(), serverDtls.getIp(), serverDtls.getPort());
                            createBackupHostLkp();
                            deleteSuccessful = deleteCurrentHost(local_hostname,partitionsToBeDistributedMap);

                        }

                        if (deleteSuccessful) {
                            System.out.println("delete request completed successfully");
                            nodeDeleted = deleteSuccessful;
                            if (tupleSocket != null) {
                                tupleSocket.close();
                            }
                            break;
                        }

                        System.out.println("delete request completed successfully");
                        System.out.println("linda>");

                    } //if delete end
                    else if (input.startsWith("in") | input.startsWith("rd")) {
                        //System.out.println("IN/RD tuple Exact match command received");
                        String[] formatted_string = new String[3];
                        formatted_string = formatString(input, -1); //required only concate string

                        if (!input.contains("?")) { //Exact Match
                            int keyNumber = getMD5(formatted_string[1]);
                            //System.out.println("keyNumber " + keyNumber);

                            System.out.println("Thread blocked till data avaialble");
                            int find_host_toStoreData = -1;
                            boolean foundDataSuccess = false;
                            boolean msgWritten = false;


                            while (true && !nodeDeleted) {
                                int find_partition_id = (keyNumber/myTopology.range);
                                int findOrigHost =  myTopology.getPartitionToNode().get(find_partition_id);
                                int findBkpHost = hostid_backuphostid.get(findOrigHost);

                                //System.out.println("Requested data should be on Orighost " + findOrigHost + " backuphost " + findBkpHost);

                                for (int i = 0; i <= 1; i++) {
                                    if (i == 0) {
                                        find_host_toStoreData = findOrigHost; // first search data from original host
                                    } else if (i == 1) {
                                        find_host_toStoreData = findBkpHost; // search data from Backup host if original host found crash
                                    }

                                    if (find_host_toStoreData != local_id) {
                                        //while (true) {
                                        try {
                                            String other_host_ip = connected_host_details.get(find_host_toStoreData).getIp();
                                            int other_host_port = connected_host_details.get(find_host_toStoreData).getPort();
                                            //System.out.println("connecting with hostID " + find_host_toStoreData + " IP " + other_host_ip + " port" + other_host_port);

                                            connectionSocket = new Socket(other_host_ip, other_host_port); //connect with other host
                                            BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                            PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);
                                            String line = "";

                                            if (input.startsWith("in") && (i == 0)) {
                                                output.println("inEM " + input);
                                                //System.out.println("sending inEM request");
                                            } else if (input.startsWith("in") && (i == 1)) {
                                                output.println("inEMBKP " + input);
                                                //System.out.println("sending inEMBKP request");
                                            } else if (input.startsWith("rd") && (i == 0)) {
                                                output.println("rdEM " + input);
                                                //System.out.println("sending rdEM request");
                                            } else if (input.startsWith("rd") && (i == 1)) {
                                                output.println("rdEMBKP " + input);
                                                //System.out.println("sending rdEMBKP request");
                                            }

                                            boolean close = false;
                                            boolean msgPrinted = false;

                                            String read_response = in.readLine();
                                            if (read_response.equals("EMDataAvailable")) {
                                                System.out.println("get tuple (" + formatted_string[2] + ") on " + other_host_ip + " port " + other_host_port+" partition "+find_partition_id);
                                                close = true;
                                                foundDataSuccess = true;
                                                //break;
                                            } else {
                                                msgPrinted = true;
                                                break; //break for loop and read again
                                            }

                                            if (!close) {
                                                System.out.println("tuple on " + other_host_ip + " on port " + other_host_port + " not In read properly");
                                            }
                                            in.close();
                                            output.close();
                                            connectionSocket.close();

                                        } catch (IOException e) {
                                            //e.printStackTrace();
                                            System.out.println("Original host is not responding.Trying to fetch data from the backup host");
                                            foundDataSuccess = false;
                                            continue;
                                        }
                                        //}//while

                                        if (foundDataSuccess) {
                                            break; //break for loop
                                        } else {
                                            System.out.println("Original host is not responding.Trying to fetch data from the backup host");
                                        }


                                    } else { //if tuple stored at local host

                                        //System.out.println("IN Exact Match at local host");
                                        if (!input.contains("?")) {
                                            boolean msgPrinted = false;
                                            while (true && !nodeDeleted) {
                                                if (store_exact_data_cnt.containsKey(formatted_string[1])) {
                                                    System.out.println("get tuple (" + formatted_string[2] + ") on " + local_ip + " port " + local_port);
                                                    if (input.startsWith("in")) {
                                                        removeTuple(input);
                                                        String temp = "(" + formatted_string[2] + ")";
                                                        String filpathtoremovetuple = absolutePathTuple+"/partition_"+find_partition_id+".txt";
                                                        removeTupleFromFile(filpathtoremovetuple, temp);
                                                        serverDetails bkphostdtls = connected_host_details.get(findBkpHost);
                                                        removeDataFromBackupHost(temp,bkphostdtls.getIp(),bkphostdtls.getPort());
                                                    }
                                                    foundDataSuccess = true;
                                                    break;
                                                } else {
                                                    foundDataSuccess = false;
                                                    if (!msgPrinted) {
                                                        System.out.println("Data not avaiable");
                                                        System.out.println("Thread blocked till data available");
                                                        msgPrinted = true;
                                                    }
                                                    Thread.sleep(1000);
                                                }

                                            }
                                        }

                                    }

                                } //for loop end

                                if (foundDataSuccess) {
                                    break;
                                } else {

                                    if (!msgWritten) {
                                        System.out.println("Data not available in Tupple Space");
                                        System.out.println("Thread blocked till data available");
                                    }
                                    Thread.sleep(5000);
                                }

                            } /// end of while

                        }
                        //IN Variable Match
                        else {

                            boolean messageWrite = false;
                            System.out.println("Thread blocked till data available");


                            // Variable Match Check local Machine First
                            while (true && !nodeDeleted) {
                                //System.out.println("IN data type " + formatted_string[0] + " data " + formatted_string[1]);

                                String[] parsedData = formatted_string[2].split("\\,");
                                HashSet<Integer> findTupleID = new HashSet<Integer>();
                                boolean found = false;
                                //while (!found) {
                                for (int i = 0; i < parsedData.length; i++) {
                                    if (!parsedData[i].trim().contains("?")) {
                                        if (posting_list.containsKey(parsedData[i].trim())) {
                                            List<postingListData> l = posting_list.get(parsedData[i].trim());
                                            for (postingListData posting : l) {
                                                if (i == posting.term_postion) {
                                                    if (!findTupleID.contains(posting.term_tuple_id)) {
                                                        findTupleID.add(posting.term_tuple_id);
                                                        //System.out.println("Found " + parsedData[i].trim() + " tuple " + posting.term_tuple_id + " position " + posting.term_postion);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                if (findTupleID.size() > 0) {
                                    for (Integer t : findTupleID) {
                                        String[] split_tuple_data = store_id_OrigData.get(t).split("\\,");
                                        boolean match = false;
                                        if (formatted_string[0].equals(store_id_type.get(t))) {
                                            for (int i = 0; i < parsedData.length; i++) {
                                                if (parsedData[i].trim().contains("?") | (parsedData[i].trim() == split_tuple_data[i].trim())) {
                                                    match = true;
                                                    found = true;
                                                } else {
                                                    match = false;
                                                    found = false;
                                                }
                                            }
                                            if (match) {
                                                break;
                                            }

                                        }
                                    }
                                } else {
                                    boolean match = false;
                                    if (store_type_data.containsKey(formatted_string[0])) {
                                        store_type_data.get(formatted_string[0]).get(0); // handle search any data in a particular format (?,?,?) case.
                                        match = true;
                                        found = true;
                                    } else {
                                        match = false;
                                        found = false;
                                    }
                                }

                                if (found) {
                                    System.out.println("get tuple " + store_type_data.get(formatted_string[0]).get(0) + " on " + local_ip + " port " + local_port);

                                    if (input.startsWith("in")) {
                                        String temp = store_type_data.get(formatted_string[0]).get(0);
                                        String[] formatted_tuple = new String[3];
                                        formatted_tuple = formatString(temp, -1);
                                        int keyNumber = getMD5(formatted_tuple[1]);
                                        //System.out.println("keyNumber " + keyNumber);
                                        int find_partition_id = (keyNumber/myTopology.range);
                                        int findOrigHost =  myTopology.getPartitionToNode().get(find_partition_id);
                                        int findBkpHost = hostid_backuphostid.get(findOrigHost);
                                        //System.out.println(temp+" available on partition "+find_partition_id+" orig host "+findOrigHost+" backup host "+findBkpHost);

                                        removeTuple(temp);
                                        String pathtoRemoveTuple = absolutePathTuple+"/partition_"+find_partition_id+".txt";
                                        removeTupleFromFile(pathtoRemoveTuple, temp);
                                        String hostIP = connected_host_details.get(hostid_backuphostid.get(local_id)).getIp();
                                        int hostPort = connected_host_details.get(hostid_backuphostid.get(local_id)).getPort();
                                        removeDataFromBackupHost(temp,hostIP,hostPort);
                                        //Handle to delete data from backup host for IN command
                                    }
                                    break;
                                }
                                System.out.println("Data not found at local machine");
                                if (!found) {
                                    boolean close = false;

                                    for (serverDetails serverDetails : connected_host_details.values()) {
                                        if (serverDetails.getHostID() != local_id) {
                                            String other_host_ip = null;
                                            int other_host_port = -1;
                                            for (int i = 0; i <= 1; i++) {

                                                try {
                                                    if (i == 0) {
                                                        other_host_ip = serverDetails.getIp();
                                                        other_host_port = serverDetails.getPort();
                                                    } else if (i == 1) {
                                                        other_host_ip = connected_host_details.get(hostid_backuphostid.get(serverDetails.getHostID())).getIp();
                                                        other_host_port = connected_host_details.get(hostid_backuphostid.get(serverDetails.getHostID())).getPort();
                                                    }


                                                    //System.out.println("connecting with serverDetails = " + other_host_ip + " " + other_host_port);


                                                    connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                                    BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);


                                                    String line = "";

                                                    if (input.startsWith("in") && (i == 0)) {
                                                        output.println("inVM " + input);
                                                        //System.out.println("sending inVM request");
                                                    } else if (input.startsWith("in") && (i == 1)) {
                                                        output.println("inVMBKP " + input);
                                                        //System.out.println("sending inVMBKP request");
                                                    } else if (input.startsWith("rd") && (i == 0)) {
                                                        output.println("rdVM " + input);
                                                        //System.out.println("sending rdVM request");
                                                    } else if (input.startsWith("rd") && (i == 1)) {
                                                        output.println("rdVMBKP " + input);
                                                        //System.out.println("sending rdVMBKP request");
                                                    }

                                                    //output.println("inVM " + input); //send number of lines are going to transfer.

                                                    Thread.sleep(500);
                                                    String read_response = in.readLine();
                                                    //System.out.println("response " + read_response);

                                                    if (read_response.equals("VMDataAvailable")) {
                                                        String response_2 = in.readLine();
                                                        String split[] = response_2.split("/");
                                                        System.out.println("get tuple " + response_2 + " on " + serverDetails.getIp() + " port " + serverDetails.getPort()+" partition "+split[1]);
                                                        close = true;
                                                        in.close();
                                                        output.close();
                                                        break; //exit from inner for loop i
                                                    }

                                                    close = false;
                                                    in.close();
                                                    output.close();
                                                    connectionSocket.close();


                                                } catch (IOException e) {
                                                    //e.printStackTrace();
                                                    System.out.println("Original host is not responding. Trying to fetch data from backup host");
                                                    close = false;
                                                    connectionSocket.close();
                                                    continue;
                                                }

                                            }
                                        }

                                        if (close) {
                                            break;  //exit from outer for loop serverDetails
                                        }
                                    }

                                    if (close) {
                                        break; //exit from outer while loop
                                    }

                                }

                                if (!messageWrite) {
                                    System.out.println("Data not avaialable in tuple space");
                                    System.out.println("Thread blocked till data get avaialable");
                                }
                                Thread.sleep(5000);

                            } // end of inner while true

                        }

                        System.out.println("linda>");

                    } //if "IN" end
                    // try end

                } catch (Exception e) {
                    //e.printStackTrace();
                    System.out.println("Error in run socket connection broke");
                } finally {
                    if (connectionSocket != null) {
                        connectionSocket = null;
                    }
                }
            } // end of while true
        }
    }

    /*server class to which will serve client requests*/

    private static class ServerListener implements Runnable { //Server acceptor
        private Socket socket;
        private String hostsFilePath;
        private Socket connectionSocket;
        private ServerSocket server;

        public ServerListener(Socket socket, String hostsFilePath, ServerSocket server) {
            this.socket = socket;
            this.hostsFilePath = hostsFilePath;
            this.server = server;
        }

        public static void processingDelay(int msec) throws InterruptedException {
            Thread.sleep(msec);
        }

        public void run() {
            String line;
            BufferedReader in = null;
            PrintWriter out = null;
            try {
                in = new BufferedReader(new InputStreamReader(socket.getInputStream())); //wait for Input message
                out = new PrintWriter(socket.getOutputStream(), true);

            } catch (IOException e) {
                System.out.println("in or out failed");
                throw new RuntimeException();
            }

            try {
                String inputString = in.readLine();
                System.out.println("Request received " + inputString);
                String[] parsedArray = inputString.split("\\s+");

                if (parsedArray[0].equalsIgnoreCase("add")) {
                    String response = "";
                    while ((response = in.readLine()) != null) {
                        if (response.equals("end")) {
                            break; // stop reading;
                        }
                        if (!response.equals("null")) {
                            String[] split_input = response.split("\\s+");
                            int hostId = Integer.parseInt(split_input[0]);
                            String hostname = split_input[1];
                            String ip = split_input[2];
                            int port = Integer.parseInt(split_input[3]);
                            serverDetails currentServer = new serverDetails(hostId, hostname, ip, port);

                            if (hostId == 0) {
                                master_id = hostId;
                                master_ip = ip;
                                master_port = port;
                                System.out.println("Master of this host : ID " + master_id + " IP " + master_ip + " Port " + master_port);
                            }

                            System.out.println("After adding host : ID "+currentServer.getHostID()+" Name "+currentServer.getHostName()+" IP "+currentServer.getIp()+" Port "+currentServer.getPort());

                            myTopology.addNewHost(currentServer);
                            myTopology.printNodeToPartitions();
                            myTopology.printPartitionsToNode();
                            System.out.println();

                            connected_host_details.put(hostId, currentServer);
                            addHostDetails(hostId, hostname, ip, port, hostsFilePath);
                            hostname_id_map.put(hostname, hostId);

                            if ((ip.equals(local_ip)) && (port == local_port)) {
                                local_id = currentServer.getHostID();
                                System.out.println("Local ID host current host " + local_id);
                                myServer.setHostID(currentServer.getHostID());
                            }
                        }
                    }
                    System.out.print("host information Added successfully");
                    out.println("added");
                    out.flush();
                    total_host_in_ds = connected_host_details.size(); //connected hosts in network
                    createBackupHostLkp();
                    try {
                        Thread.sleep(1000);
                    }catch (Exception e){
                        System.out.println("serverlistner : add thread intrupted");
                    }
                    createTupleDirectory(local_hostname);
                    System.out.println("Connected hosts in system " + total_host_in_ds);
                    System.out.println("linda>");
                } else if (parsedArray[0].equalsIgnoreCase("addNew")) {
                    out.println("added");
                    out.flush();

                    try {
                        serverDetails otherServer = new serverDetails(Integer.parseInt(parsedArray[1]), parsedArray[2], parsedArray[3], Integer.parseInt(parsedArray[4]));
                        //myTopology = new TopologyInfo(otherServer);
                        List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();
                        List<Integer> partitionsToBeCopied = myTopology.addNewHost(otherServer);
                        totalpartitionListToTransfer.add(partitionsToBeCopied);
                        System.out.println("After adding host : ID " + otherServer.getHostID() + " Name " + otherServer.getHostName() + " IP " + otherServer.getIp() + " Port " + otherServer.getPort());
                        myTopology.printNodeToPartitions();
                        myTopology.printPartitionsToNode();
                        System.out.println();

                        connected_host_details.put(Integer.parseInt(parsedArray[1]), otherServer);
                        addHostDetails(Integer.parseInt(parsedArray[1]), parsedArray[2], parsedArray[3], Integer.parseInt(parsedArray[4]), hostsFilePath);
                        hostname_id_map.put(parsedArray[2], Integer.parseInt(parsedArray[1]));
                        System.out.println("Added New host " + Integer.parseInt(parsedArray[1]) + " " + parsedArray[2] + " " + parsedArray[3] + " " + Integer.parseInt(parsedArray[4]));
                        total_host_in_ds = connected_host_details.size(); //connected hosts in network
                        boolean backupHostChange = createBackupHostLkp();
                        Thread.sleep(1000);
                        transferPartitionToNewHost(partitionsToBeCopied, otherServer.getIp(), otherServer.getPort());
                        System.out.println("Partition data transferred to new host successfully");
                        if (backupHostChange) {
                            TransferOriginalTupleData(totalpartitionListToTransfer);
                        }
                        else {
                            removePartitionDataFromBackupHost(totalpartitionListToTransfer);
                        }
                    }catch (Exception e){
                        //e.printStackTrace();
                        System.out.println("ServerListner : addNew Request failed");
                    }
                    System.out.println("Connected hosts in system " + total_host_in_ds);
                    System.out.println("linda>");

                } else if (parsedArray[0].equalsIgnoreCase("newRequest")) {

                    out.println("newRequestServed");
                    out.flush();

                    Integer host_id_gererate = (connected_host_details.size() ==0 ? 0 :Collections.max(connected_host_details.keySet()));
                    System.out.println("Collection max Keyset value "+host_id_gererate);
                    while (connected_host_details.containsKey(host_id_gererate)) {
                        host_id_gererate++;
                    }


                    int other_host_id = host_id_gererate;

                    List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();
                    serverDetails otherServer = new serverDetails(other_host_id, parsedArray[1], parsedArray[2], Integer.parseInt(parsedArray[3]));
                    List<Integer> partitionsToBeCopied=myTopology.addNewHost(otherServer);
                    totalpartitionListToTransfer.add(partitionsToBeCopied);
                    System.out.println("After adding host : ID "+otherServer.getHostID()+" Name "+otherServer.getHostName()+" IP "+otherServer.getIp()+" Port "+otherServer.getPort());
                    myTopology.printNodeToPartitions();
                    myTopology.printPartitionsToNode();
                    System.out.println();

                    connected_host_details.put(other_host_id,otherServer);
                    addHostDetails(other_host_id, parsedArray[1], parsedArray[2], Integer.parseInt(parsedArray[3]), hostsFilePath);
                    hostname_id_map.put(parsedArray[1], other_host_id);


                    for (serverDetails serverDetails : connected_host_details.values()) {
                        try {
                            if (serverDetails.getHostID() != local_id && (serverDetails.getHostID() != other_host_id)) {
                                connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                BufferedReader in_rq = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                output.println("addNew " + other_host_id + " " + parsedArray[1] + " " + parsedArray[2] + " " + Integer.parseInt(parsedArray[3])); //send number of lines are going to transfer.

                                String read_response = in_rq.readLine();

                                if (read_response.equals("added")) {
                                    in_rq.close();
                                    output.close();
                                }

                                in_rq.close();
                                output.close();
                                connectionSocket.close();
                            } else if ((serverDetails.getHostID() != local_id) && (serverDetails.getHostID() == other_host_id)) {
                                connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                BufferedReader in_rq = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                String line1 = "";
                                BufferedReader br = new BufferedReader(new FileReader(hostsFilePath));
                                try {
                                    output.println("add hosts"); //send number of lines are going to transfer.
                                    while ((line1 = br.readLine()) != null) {
                                        output.println(line1);
                                    }
                                    output.println("end");
                                } catch (IOException e) {
                                    System.out.println("ServerListner : newRequest Request failed");
                                    //e.printStackTrace();
                                } finally {
                                    br.close();
                                }

                                boolean close = false;
                                int k = 5;
                                while (k >= 0) {
                                    String read_response = in_rq.readLine();
                                    //System.out.println("response " + read_response);
                                    if (read_response.equals("added")) {
                                        close = true;
                                        break;
                                    }
                                    //Thread.sleep(5000);
                                    k--;
                                }
                                if (!close) {
                                    System.out.println("Connection with " + serverDetails.getIp() + " on port " + serverDetails.getPort() + " unsuccessful");
                                }

                                transferTopologyInformation(serverDetails.getIp(),serverDetails.getPort());
                                //System.out.println("Partition information transferred to new host successfully");
                                transferPartitionToNewHost(partitionsToBeCopied,serverDetails.getIp(),serverDetails.getPort());
                                //System.out.println("Partition data transferred to new host successfully");

                                in.close();
                                output.close();
                                connectionSocket.close();

                            }
                        } catch (IOException e) {
                            //e.printStackTrace();
                            System.out.println("ServerListner : newRequest Request failed");
                            continue;
                        }

                    }


                    total_host_in_ds = connected_host_details.size(); //Strore number of hosts in system
                    System.out.println("New hosts added successfully");
                    System.out.println("total host in system " + total_host_in_ds);
                    boolean backupHostChange = createBackupHostLkp();
                    if(backupHostChange){
                        TransferOriginalTupleData(totalpartitionListToTransfer);
                    }
                    else {
                        removePartitionDataFromBackupHost(totalpartitionListToTransfer);
                    }
                    System.out.println("linda>");

                } else if (inputString.startsWith("updateMaster")) {
                    master_id = Integer.parseInt(parsedArray[1]);
                    master_ip = parsedArray[2];
                    master_port = Integer.parseInt(parsedArray[3]);
                    out.println("updated");
                    out.flush();
                    System.out.println("Master host details updated successfully");
                    System.out.println("linda>");
                } else if (inputString.startsWith("elect")) {

                    String result_ip = parsedArray[3].compareToIgnoreCase(String.valueOf(local_port)) >= 0 ? parsedArray[3] : String.valueOf(local_port);

                    if (result_ip.equals(String.valueOf(local_port))) {
                        out.println(local_id + " " + local_ip + " " + local_port);
                        out.flush();
                        System.out.println("response sent to elect master " + local_id + " " + local_ip + " " + local_port);

                    } else {
                        out.println(parsedArray[1] + " " + parsedArray[2] + " " + parsedArray[3]);
                        out.flush();
                        System.out.println("response sent to elect master " + parsedArray[1] + " " + parsedArray[2] + " " + parsedArray[3]);

                    }

                } else if (inputString.startsWith("becomeMaster")) {
                    master_ip = local_ip;
                    master_port = local_port;
                    master_id = local_id;

                    String[] parsedInputfinal = inputString.split("\\s+");
                    String[] parsedInputExtract = new String[parsedInputfinal.length - 2];
                    List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();


                    for (int i = 0; i < parsedInputExtract.length; i++) {
                        parsedInputExtract[i] = parsedInputfinal[i + 2].substring(parsedInputfinal[i + 2].indexOf("(") + 1, parsedInputfinal[i + 2].indexOf(")"));
                    }



                    //block to add all host in direcotry at local
                    for (int i = 0; i < parsedInputExtract.length; i++) {
                        String[] parsedInput = parsedInputExtract[i].split("\\,");

                        Integer host_id_gererate = (connected_host_details.size() ==0 ? 0 :Collections.max(connected_host_details.keySet()));
                        while (connected_host_details.containsKey(host_id_gererate)) {
                            host_id_gererate++;
                        }

                        int other_host_id = host_id_gererate;

                        serverDetails otherServer = new serverDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]));
                        List<Integer> partitionsToBeCopied=myTopology.addNewHost(otherServer);
                        totalpartitionListToTransfer.add(partitionsToBeCopied);
                        System.out.println("After adding host : ID "+otherServer.getHostID()+" Name "+otherServer.getHostName()+" IP "+otherServer.getIp()+" Port "+otherServer.getPort());
                        myTopology.printNodeToPartitions();
                        myTopology.printPartitionsToNode();
                        System.out.println();

                        connected_host_details.put(other_host_id,otherServer);
                        addHostDetails(other_host_id, parsedInput[0], parsedInput[1], Integer.parseInt(parsedInput[2]), hostsFilePath);
                        hostname_id_map.put(parsedInput[0], other_host_id);


                        for (serverDetails serverDetails : connected_host_details.values()) {
                            try {
                                if (serverDetails.getHostID() != local_id && (serverDetails.getHostID() != other_host_id)) {
                                    connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                    BufferedReader in_ms = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                    output.println("addNew " + other_host_id + " " + parsedInput[0] + " " + parsedInput[1] + " " + Integer.parseInt(parsedInput[2])); //send number of lines are going to transfer.


                                    String read_response = in_ms.readLine();

                                    if (read_response.equals("added")) {
                                        in_ms.close();
                                        output.close();
                                    }

                                    in_ms.close();
                                    output.close();
                                    connectionSocket.close();
                                } else if ((serverDetails.getHostID() != local_id) && (serverDetails.getHostID() == other_host_id)) {
                                    connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                    BufferedReader in_bec = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                    PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                    String line2 = "";
                                    BufferedReader br = new BufferedReader(new FileReader(hostsFilePath));
                                    try {
                                        output.println("add hosts"); //send number of lines are going to transfer.
                                        while ((line2 = br.readLine()) != null) {
                                            output.println(line2);
                                        }
                                        output.println("end");
                                    } catch (IOException e) {
                                        System.out.println("ServerListner : becomeMaster Request failed");
                                        //e.printStackTrace();
                                    } finally {
                                        br.close();
                                    }

                                    boolean close = false;
                                    int k = 5;
                                    while (k >= 0) {
                                        String read_response = in_bec.readLine();
                                        //System.out.println("response " + read_response);
                                        if (read_response.equals("added")) {
                                            close = true;
                                            break;
                                        }
                                        //Thread.sleep(5000);
                                        k--;
                                    }
                                    if (!close) {
                                        System.out.println("Connection with " + serverDetails.getIp() + " on port " + serverDetails.getPort() + " unsuccessful");
                                    }

                                    transferTopologyInformation(serverDetails.getIp(),serverDetails.getPort());
                                    //System.out.println("Partition information transferred to new host successfully");
                                    transferPartitionToNewHost(partitionsToBeCopied,serverDetails.getIp(),serverDetails.getPort());
                                    //System.out.println("Partition data transferred to new host successfully");


                                    in_bec.close();
                                    output.close();
                                    connectionSocket.close();

                                }
                            } catch (IOException e) {
                                //e.printStackTrace();
                                System.out.println("ServerListner : socket connection failed");
                                continue;
                            }

                        }

                    }



                     /*block to inform all hosts to update Master */
                    for (serverDetails serverDetails : connected_host_details.values()) {
                        try {
                            if (serverDetails.getHostID() != local_id) {
                                connectionSocket = new Socket(serverDetails.getIp(), serverDetails.getPort()); //connect with other host
                                BufferedReader in_rq = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                                PrintWriter output = new PrintWriter(connectionSocket.getOutputStream(), true);

                                output.println("updateMaster " + master_id + " " + master_ip + " " + master_port);
                                String read_response = in_rq.readLine();

                                if (read_response.equals("updated")) {
                                    //String response_2 = in.readLine();
                                    //System.out.println("response " + response_2);
                                    in_rq.close();
                                    output.close();
                                }

                                in_rq.close();
                                output.close();
                                connectionSocket.close();
                            }
                        } catch (IOException e) {
                            //e.printStackTrace();
                            System.out.println("ServerListner : socket connection failed");
                            continue;
                        }

                    }

                    System.out.println("informed successfully to each host for new master ");
                    boolean backupHostChange = createBackupHostLkp();
                    if(backupHostChange){
                        TransferOriginalTupleData(totalpartitionListToTransfer);
                    }
                    else {
                        removePartitionDataFromBackupHost(totalpartitionListToTransfer);
                    }
                    total_host_in_ds = connected_host_details.size(); //Strore number of hosts in system
                    System.out.println("New hosts added successfuly");
                    System.out.println("total host in system " + total_host_in_ds);
                    System.out.println("linda>");

                } //end becomeMaster
                else if (inputString.startsWith("transferTopology")) {

                    //System.out.println("transferTopology request received");
                    List<Integer> PartitionToNode = new ArrayList<Integer>();
                    String response = "";
                    while ((response = in.readLine()) != null) {
                        if (response.equals("end")) {
                            break; // stop reading;
                        }
                        if (!response.equals("null")) {
                            String[] split_input = response.split("\\s+");
                            PartitionToNode.add(Integer.parseInt(split_input[0]),Integer.parseInt(split_input[1]));
                        }
                    }

                    myTopology.setPartitionToNode(PartitionToNode);
                    myTopology.convertInputListOfPartitionsToHostToAMap();
                    myTopology.printPartitionsToNode();
                    myTopology.printNodeToPartitions();
                    out.println("partitionAdded");
                    out.flush();
                } //end transferTopology

                else if (parsedArray[0].equalsIgnoreCase("store")) {
                    //System.out.println("Received store request");
                    while (store_id_OrigData.containsKey(tuple_id)){
                        tuple_id++;
                    }
                    String str = inputString.substring(inputString.indexOf("out"), inputString.indexOf(")") + 1);
                    String[] formatted_data = new String[3];
                    formatted_data = formatString(str, tuple_id);
                    String temp = "(" + formatted_data[2] + ")";

                    int keyNumber = getMD5(formatted_data[1]);
                    int find_partition_id = (keyNumber/myTopology.range);
                    int findOrigHost =  myTopology.getPartitionToNode().get(find_partition_id);
                    int findBkpHost = hostid_backuphostid.get(findOrigHost);

                    //System.out.println("find_partition_id " + find_partition_id + " findOrigHost "+findOrigHost+" findBkpHost "+findBkpHost);
                    String partitionFilepath = absolutePathTuple+"/partition_"+find_partition_id+".txt";
                    addtuples(temp, partitionFilepath);
                    out.print("stored");
                    out.flush();
                    System.out.println("Tuple (" + formatted_data[2] + ") added successfully on partition "+find_partition_id);
                    System.out.println("linda>");
                }
                else if (inputString.startsWith("transferPartition")) {
                    //System.out.println("transferPartition request received");
                    List<String> data = new ArrayList<String>();
                    List<String> list = new ArrayList<String>();

                    String response = "";
                    while ((response = in.readLine()) != null) {
                        if (response.equals("end")) {
                            break; // stop reading;
                        }
                        if (!response.equals("null")) {
                            if(response.startsWith("addPartition")){
                                String[] split = response.split("/");
                                //System.out.println("transferPartition split "+split[1]);
                                list.add(split[1]);
                            }
                            else {
                                while (store_id_OrigData.containsKey(tuple_id)) {
                                    tuple_id++;
                                }
                                String[] str = response.trim().split("/");
                                //System.out.println("transferPartition data received from file " + str[0] + " " + str[1]);
                                String[] formatted_data = new String[3];
                                formatted_data = formatString(str[1], tuple_id);
                                String temp = "(" + formatted_data[2] + ")";
                                data.add(str[0]+"/"+temp);

                            }
                        }
                    }


                    boolean flg = false;
                    for (int i =0;i<list.size();i++) {

                        try {

                            File file1 = new File("/tmp/nshah/linda/" + local_hostname + "/partition_" + list.get(i) + ".txt");
                            if (!file1.exists()) {
                                if (file1.createNewFile()) {
                                    //System.out.println("creating new file " + list.get(i));
                                    flg = true;
                                }
                            }

                        } catch (Exception e) {
                            //e.printStackTrace();
                            System.out.println("ServerListner : Transfer partition failed");
                        }

                    }
                    //createTupleDirectory(local_hostname);
                    if(data.size()!=0) {
                        for (int j =0;j<data.size();j++) {
                            try {
                                String[] str = data.get(j).trim().split("/");
                                String partitionFilepath = absolutePathTuple + "/partition_" + str[0] + ".txt";
                                addtuples(str[1], partitionFilepath);
                            } catch (Exception e) {
                                //e.printStackTrace();
                                System.out.println("ServerListner : Transfer partition failed");
                            }
                        }
                    }

                    out.print("copied");
                    out.flush();
                    List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();
                    TransferOriginalTupleData(totalpartitionListToTransfer);
                    System.out.println("partitions data copied successfully");
                    System.out.println("linda>");
                }
                else if (parsedArray[0].equalsIgnoreCase("transferOrigTuple")) {
                    //System.out.println("transferOrigTuple request received");

                    String response = "";
                    while ((response = in.readLine()) != null) {
                        if (response.equals("end")) {
                            break; // stop reading;
                        }
                        if (!response.equals("null")) {
                            while (store_id_OrigData.containsKey(tuple_id)) {
                                tuple_id++;
                            }
                            String str = response.trim();//inputString.substring(inputString.indexOf("out"), inputString.indexOf(")") + 1);
                            String[] formatted_data = new String[3];
                            formatted_data = formatString(str, tuple_id);
                            int keyNumber = getMD5(formatted_data[1]);
                            int find_partition_id = (keyNumber/myTopology.range);
                            String filePath = "/tmp/nshah/linda/"+local_hostname+"/partition_"+find_partition_id+".txt";
                            String temp = "(" + formatted_data[2] + ")";
                            addtuples(temp, filePath);
                        }
                    }

                    out.print("stored");
                    out.flush();
                    //System.out.println("Original Tuple data transfer done successfully");
                    System.out.println("linda>");
                } else if (parsedArray[0].equalsIgnoreCase("transferBKPTuple")) {
                    //System.out.println("transferBKPTuple request received");

                    String response = "";
                    while ((response = in.readLine()) != null) {
                        if (response.equals("end")) {
                            break; // stop reading;
                        }
                        if (!response.equals("null")) {
                            while (store_id_OrigData.containsKey(tuple_id)) {
                                tuple_id++;
                            }
                            String str = response.trim();
                            String[] formatted_data = new String[3];
                            formatted_data = formatString_bkp(str, tuple_id);
                            String temp = "(" + formatted_data[2] + ")";
                            addtuples_bkp(temp, absolutePathBackupTuple);
                        }
                    }

                    out.print("stored");
                    out.flush();
                    //System.out.println("Backup Tuple data transfer done successfully");
                    System.out.println("linda>");
                } else if (parsedArray[0].equalsIgnoreCase("BackupStore")) {
                    //System.out.println("Received BackupStore request " + inputString);
                    while (store_id_OrigData.containsKey(tuple_id)){
                        tuple_id++;
                    }
                    String str = inputString.substring(inputString.indexOf("out"), inputString.indexOf(")") + 1);
                    //System.out.println(str);
                    String[] formatted_data = new String[3];
                    formatted_data = formatString_bkp(str, tuple_id);
                    String temp = "(" + formatted_data[2] + ")";
                    addtuples_bkp(temp, absolutePathBackupTuple);
                    out.print("stored");
                    out.flush();
                    System.out.println("Tuple (" + formatted_data[2] + ") added on backup host successfully");
                    System.out.println("linda>");
                }
                else if (parsedArray[0].equalsIgnoreCase("backupHostDeleteTuple")) {
                    //System.out.println("backupHostDeleteTuple : request received");

                    String response = "";
                    while ((response = in.readLine()) != null) {
                        if (response.equals("end")) {
                            break; // stop reading;
                        }

                        if (!response.equals("null")) {
                            //System.out.println("transferOrigTuple data received from file " + response.trim());
                            String str = response.trim();
                            //System.out.println("Removing line from backup host : "+str);
                            removeTuple_bkp(str);
                            removeTupleFromFile_bkp(absolutePathBackupTuple,str);
                        }
                    }

                    out.print("removed");
                    out.flush();
                    System.out.println("tuple deleted from backup host");
                    System.out.println("linda>");
                }
                else if (parsedArray[0].equalsIgnoreCase("deleteRequest")) {
                    //System.out.println("Received deleteRequest request " + inputString);
                    int hostID = Integer.parseInt(parsedArray[1]);
                    String hostName = parsedArray[2];
                    String hostIP = parsedArray[3];
                    int hostPort = Integer.parseInt(parsedArray[4]);

                    serverDetails serverDtls = new serverDetails(hostID,hostName,hostIP,hostPort);


                    if (Integer.parseInt(parsedArray[1]) != local_id) {
                        try {
                            System.out.println("Received deleteRequest request is not local ");
                            Map<Integer, List<Integer>> partitionTobeAdded = myTopology.deleteHost(serverDtls);
                            myTopology.printNodeToPartitions();
                            myTopology.printPartitionsToNode();
                            deleteHostInfoFromLocal(hostID,hostName,hostIP,hostPort);
                            boolean bkpHostChange = createBackupHostLkp();

                            //System.out.println("bkpHostChange " + bkpHostChange);
                            if (bkpHostChange && (!partitionTobeAdded.containsKey(local_id))) {
                                //System.out.println("local host backup host before : " + localhost_bkpHostID);
                                List<List<Integer>> totalpartitionListToTransfer = new ArrayList<List<Integer>>();
                                TransferOriginalTupleData(totalpartitionListToTransfer);
                                localhost_bkpHostID = hostid_backuphostid.get(local_id);
                                //System.out.println("local host backup host after : " + localhost_bkpHostID);
                            }
                            else if((!bkpHostChange) && (partitionTobeAdded.containsKey(local_id))) {
                                List<List<Integer>> newPartitionList = new ArrayList<List<Integer>>();
                                TransferOriginalTupleData(newPartitionList);
                            }
                            else if((bkpHostChange) && (partitionTobeAdded.containsKey(local_id))) {
                                List<List<Integer>> newPartitionList = new ArrayList<List<Integer>>();
                                TransferOriginalTupleData(newPartitionList);
                            }

                            out.print("deleted");
                            out.flush();
                            System.out.println("Host information deleted successfully");
                            System.out.println("linda>");
                        }catch (Exception e){
                            //e.printStackTrace();
                            System.out.println("serverlistner : delete Request failed");
                        }
                    } else if (Integer.parseInt(parsedArray[1]) == local_id) {

                        try {
                            System.out.println("Received deleteRequest request is local ");
                            Map<Integer, List<Integer>> partitionsToBeDistributedMap = myTopology.deleteHost(serverDtls);
                            myTopology.printNodeToPartitions();
                            myTopology.printPartitionsToNode();
                            for (Map.Entry<Integer, List<Integer>> nTp : partitionsToBeDistributedMap.entrySet()) {
                                int hostID1 = nTp.getKey();
                                List<Integer> partitionList = partitionsToBeDistributedMap.get(hostID1);
                                String hostIP1 = connected_host_details.get(hostID1).getIp();
                                int hostPort1 = connected_host_details.get(hostID1).getPort();
                                transferPartitionToNewHost(partitionList, hostIP1, hostPort1);

                            }

                            deleteCurrentHost(local_hostname,partitionsToBeDistributedMap);
                            // :TODO Improve error handling
                            nodeDeleted = true;
                            out.print("deleted");
                            out.flush();
                            System.out.println("Host information deleted successfully");
                            try {
                                tupleSocket.close();
                            } catch (Exception e) {
                                System.out.println("Error while closing main socket within delete node functionality");
                            }
                            //System.out.println("linda>");
                        } catch (Exception e) {
                            //e.printStackTrace();
                            System.out.println("serverlistner : delete Request failed");
                        }

                    }

                } else if (inputString.startsWith("backupHostChangeRequest")) {

                    //System.out.println("backupHostChangeRequest request received");

                    clearBackupTupleDataLocal();

                    String response = "";
                    while ((response = in.readLine()) != null) {
                        if (response.equals("end")) {
                            break; // stop reading;
                        }
                        if (!response.equals("null")) {
                            while (store_id_OrigData.containsKey(tuple_id)){
                                tuple_id++;
                            }
                            //System.out.println("transferBKPTuple data received from file " + response.trim());
                            String str = response.trim();//inputString.substring(inputString.indexOf("out"), inputString.indexOf(")") + 1);
                            //System.out.println("Data received " + str);
                            String[] formatted_data = new String[3];
                            formatted_data = formatString_bkp(str, tuple_id);
                            String temp = "(" + formatted_data[2] + ")";
                            addtuples_bkp(temp, absolutePathBackupTuple);
                        }
                    }

                    out.print("backupDataAdded");
                    out.flush();
                    System.out.println("Backup Tuple data transfer done successfully");
                    System.out.println("linda>");
                } else if (parsedArray[0].equalsIgnoreCase("inEM") | parsedArray[0].equalsIgnoreCase("rdEM")) {

                    //System.out.println("IN/RD EM request received");
                    String str = null;

                    if (inputString.startsWith("inEM")) {
                        str = inputString.substring(inputString.indexOf("in("), inputString.indexOf(")") + 1);
                    } else if (inputString.startsWith("rdEM")) {
                        str = inputString.substring(inputString.indexOf("rd("), inputString.indexOf(")") + 1);
                    }

                    //System.out.println("IN/RD EM request: str " + str);

                    String[] formatted_string = new String[3];
                    formatted_string = formatString(str, -1); //requir

                    int keyNumber = getMD5(formatted_string[1]);
                    int find_partition_id = (keyNumber/myTopology.range);
                    int findOrigHost =  myTopology.getPartitionToNode().get(find_partition_id);
                    int findBkpHost = hostid_backuphostid.get(findOrigHost);
                    //System.out.println("("+formatted_string[2]+") avaialable on partition "+find_partition_id+" orig host "+findOrigHost+" backup host "+findBkpHost);

                    boolean msgPrinted = false;

                    //System.out.println("nodeDeleted" +nodeDeleted);
                    while (true && !nodeDeleted) {
                        if (store_exact_data_cnt.containsKey(formatted_string[1])) {
                            String temp = "(" + formatted_string[2] + ")";
                            System.out.println("tuple "+temp+" found on current host : response sent to client");
                            out.println("EMDataAvailable");
                            out.flush();

                            if (inputString.startsWith("inEM")) {
                                removeTuple(str);
                                String filepathtoRemoveTuple = absolutePathTuple+"/partition_"+find_partition_id+".txt";
                                removeTupleFromFile(filepathtoRemoveTuple, temp);

                                String hostIP = connected_host_details.get(findBkpHost).getIp();
                                int hostPort = connected_host_details.get(findBkpHost).getPort();
                                removeDataFromBackupHost(temp,hostIP,hostPort);
                            }

                            break;

                        } else {
                            out.println("EMDataNotAvailable");
                            out.flush();
                            if (!msgPrinted) {
                                System.out.println("Data not available in Tuple Space");
                                System.out.println("Thread blocked till data available");
                                msgPrinted = true;
                            }
                        }

                    }

                    System.out.println("linda>");

                } else if (parsedArray[0].equalsIgnoreCase("inVM") | parsedArray[0].equalsIgnoreCase("rdVM")) {


                    //System.out.println("IN/RD VM request received");
                    String str = null;

                    if (inputString.startsWith("inVM")) {
                        str = inputString.substring(inputString.indexOf("in("), inputString.indexOf(")") + 1);
                    } else if (inputString.startsWith("rdVM")) {
                        str = inputString.substring(inputString.indexOf("rd("), inputString.indexOf(")") + 1);
                    }


                    String[] formatted_string = new String[3];
                    formatted_string = formatString(str, -1); //requir

                    //System.out.println("IN data type " + formatted_string[0] + " data " + formatted_string[1]);
                    //System.out.println("String Split " + formatted_string[2]);
                    String[] parsedData = formatted_string[2].split("\\,");
                    HashSet<Integer> findTupleID = new HashSet<Integer>();

                    boolean found = false;

                    for (int i = 0; i < parsedData.length; i++) {
                        if (!parsedData[i].trim().contains("?")) {
                            if (posting_list.containsKey(parsedData[i].trim())) {
                                List<postingListData> l = posting_list.get(parsedData[i].trim());
                                for (postingListData posting : l) {
                                    if (i == posting.term_postion) {
                                        if (!findTupleID.contains(posting.term_tuple_id)) {
                                            findTupleID.add(posting.term_tuple_id);
                                            //System.out.println("Found " + parsedData[i].trim() + " tuple " + posting.term_tuple_id + " position " + posting.term_postion);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    String foundMatchedTuple = null;
                    //System.out.println("findTupleID size " + findTupleID.size());
                    if (findTupleID.size() > 0) {
                        for (Integer t : findTupleID) {
                            String[] split_tuple_data = store_id_OrigData.get(t).split("\\,");
                            boolean match = false;
                            if (formatted_string[0].equals(store_id_type.get(t))) {
                                for (int i = 0; i < parsedData.length; i++) {
                                    if (parsedData[i].trim().contains("?") | (parsedData[i].trim() == split_tuple_data[i].trim())) {
                                        match = true;
                                        found = true;
                                    } else {
                                        match = false;
                                        found = false;
                                    }
                                }

                                if (match) {
                                    foundMatchedTuple = "(" + store_id_OrigData.get(t) + ")";
                                    break;
                                }

                            }
                        }
                    }

                    //System.out.println("foundMatchedTuple "+foundMatchedTuple);
                    if (findTupleID.size() == 0 | (findTupleID.size() > 0 && (!found))) {

                        //System.out.println("VM else");
                        boolean match = false;
                        if (store_type_data.containsKey(formatted_string[0])) {
                            //System.out.println("data available " + store_type_data.get(formatted_string[0]).get(0));
                            store_type_data.get(formatted_string[0]).get(0); // handle search any data in a particular format (?,?,?) case.
                            match = true;
                            found = true;
                            foundMatchedTuple = store_type_data.get(formatted_string[0]).get(0);

                        } else {
                            //System.out.println("Data not found else");
                            match = false;
                            found = false;
                        }
                    }




                    //System.out.println("Found " + found);
                    if (found) {

                        if (inputString.startsWith("inVM")) {
                            String[] formatted_tuple = new String[3];
                            formatted_tuple = formatString(foundMatchedTuple, -1);
                            int keyNumber = getMD5(formatted_tuple[1]);
                            int find_partition_id = (keyNumber / myTopology.range);

                            out.println("VMDataAvailable");
                            out.flush();
                            out.println(foundMatchedTuple+"/"+find_partition_id);
                            out.flush();
                            removeTuple(foundMatchedTuple);

                            int findOrigHost = myTopology.getPartitionToNode().get(find_partition_id);
                            int findBkpHost = hostid_backuphostid.get(findOrigHost);
                            String filepathToRemoveTuple = absolutePathTuple+"/partition_"+find_partition_id+".txt";
                            removeTupleFromFile(filepathToRemoveTuple, foundMatchedTuple);
                            String hostIP = connected_host_details.get(findBkpHost).getIp();
                            int hostPort = connected_host_details.get(findBkpHost).getPort();
                            removeDataFromBackupHost(foundMatchedTuple,hostIP,hostPort);
                        } else if (inputString.startsWith("rdVM")) {
                            String[] formatted_tuple = new String[3];
                            formatted_tuple = formatString(foundMatchedTuple, -1);
                            int keyNumber = getMD5(formatted_tuple[1]);
                            int find_partition_id = (keyNumber / myTopology.range);

                            out.println("VMDataAvailable");
                            out.flush();
                            out.println(foundMatchedTuple+"/"+find_partition_id);
                            out.flush();
                        }
                        System.out.println("Request data available -- response sent ");
                        System.out.println("linda>");
                    } else {
                        out.print("VMDataNotAvailable");
                        out.flush();
                        System.out.println("Read request data Not available -- response sent ");
                        System.out.println("linda>");
                    }


                }
                ////
                else if (parsedArray[0].equalsIgnoreCase("inEMBKP") | parsedArray[0].equalsIgnoreCase("rdEMBKP")) {

                    //System.out.println("IN/RD EMBKP request received");
                    String str = null;

                    if (inputString.startsWith("inEMBKP")) {
                        str = inputString.substring(inputString.indexOf("in("), inputString.indexOf(")") + 1);
                    } else if (inputString.startsWith("rdEMBKP")) {
                        str = inputString.substring(inputString.indexOf("rd("), inputString.indexOf(")") + 1);
                    }

                    String[] formatted_string = new String[3];
                    formatted_string = formatString_bkp(str, -1); //requir

                    boolean msgPrinted = false;
                    if (store_exact_data_cnt_bkp.containsKey(formatted_string[1])) {
                        System.out.println("request data "+str+" found on this backup host");
                        out.print("EMDataAvailable");
                        out.flush();

                        if (inputString.startsWith("inEMBKP")) {
                            removeTuple_bkp(str);
                            String temp = "(" + formatted_string[2] + ")";
                            removeTupleFromFile_bkp(absolutePathBackupTuple, temp);
                        }

                    } else {
                        //System.out.println("Else clause");
                        out.print("EMDataNotAvailable");
                        out.flush();
                        if (!msgPrinted) {
                            System.out.println("Data not available in Tuple Space");
                            msgPrinted = true;
                        }
                        //Thread.sleep(5000);
                    }
                    //System.out.println("Accepter read Thread blocked till data available");
                    //Thread.sleep(5000);

                    System.out.println("linda>");
                } else if (parsedArray[0].equalsIgnoreCase("inVMBKP") | parsedArray[0].equalsIgnoreCase("rdVMBKP")) {


                    //System.out.println("IN/RD VMBKP request received");
                    String str = null;

                    if (inputString.startsWith("inVMBKP")) {
                        str = inputString.substring(inputString.indexOf("in("), inputString.indexOf(")") + 1);
                    } else if (inputString.startsWith("rdVMBKP")) {
                        str = inputString.substring(inputString.indexOf("rd("), inputString.indexOf(")") + 1);
                    }

                    //System.out.println("IN/RD VMBKP request: str " + str);


                    String[] formatted_string = new String[3];
                    formatted_string = formatString_bkp(str, -1); //requir

                    //System.out.println("IN data type " + formatted_string[0] + " data " + formatted_string[1]);
                    //System.out.println("String Split " + formatted_string[2]);
                    String[] parsedData = formatted_string[2].split("\\,");
                    HashSet<Integer> findTupleID = new HashSet<Integer>();

                    boolean found = false;

                    for (int i = 0; i < parsedData.length; i++) {
                        //System.out.println("checking for " + parsedData[i]);
                        if (!parsedData[i].trim().contains("?")) {
                            if (posting_list_bkp.containsKey(parsedData[i].trim())) {
                                List<postingListData> l = posting_list_bkp.get(parsedData[i].trim());
                                for (postingListData posting : l) {
                                    if (i == posting.term_postion) {
                                        if (!findTupleID.contains(posting.term_tuple_id)) {
                                            findTupleID.add(posting.term_tuple_id);
                                            //System.out.println("Found " + parsedData[i].trim() + " tuple " + posting.term_tuple_id + " position " + posting.term_postion);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    String foundMatchedTuple = null;
                    //System.out.println("findTupleID size " + findTupleID.size());
                    if (findTupleID.size() > 0) {
                        for (Integer t : findTupleID) {
                            String[] split_tuple_data = store_id_OrigData_bkp.get(t).split("\\,");
                            boolean match = false;
                            if (formatted_string[0].equals(store_id_type_bkp.get(t))) {
                                for (int i = 0; i < parsedData.length; i++) {
                                    if (parsedData[i].trim().contains("?") | (parsedData[i].trim() == split_tuple_data[i].trim())) {
                                        match = true;
                                        found = true;
                                    } else {
                                        match = false;
                                        found = false;
                                    }
                                }

                                if (match) {
                                    foundMatchedTuple = "(" + store_id_OrigData_bkp.get(t) + ")";
                                    break;
                                }

                            }
                        }
                    }

                    //System.out.println("foundMatchedTuple "+foundMatchedTuple);
                    if (findTupleID.size() == 0 | (findTupleID.size() > 0 && (!found))) {

                        //System.out.println("VM else");
                        boolean match = false;
                        if (store_type_data_bkp.containsKey(formatted_string[0])) {
                            //System.out.println("data available " + store_type_data_bkp.get(formatted_string[0]).get(0));
                            store_type_data_bkp.get(formatted_string[0]).get(0); // handle search any data in a particular format (?,?,?) case.
                            match = true;
                            found = true;
                            foundMatchedTuple = store_type_data_bkp.get(formatted_string[0]).get(0);

                        } else {
                            //System.out.println("Data not found else");
                            match = false;
                            found = false;
                        }
                    }

                    //System.out.println("Found " + found);
                    if (found) {

                        if (inputString.startsWith("inVMBKP")) {
                            out.println("inVMDataAvailable");
                            out.flush();
                            out.println(foundMatchedTuple);
                            out.flush();
                            removeTuple_bkp(foundMatchedTuple);
                            String temp = "(" + formatted_string[2] + ")";
                            removeTupleFromFile_bkp(absolutePathBackupTuple, foundMatchedTuple);
                        } else if (inputString.startsWith("rdVMBKP")) {
                            out.println("VMDataAvailable");
                            out.flush();
                            out.println(foundMatchedTuple);
                            out.flush();
                        }
                        System.out.println("Request data available -- response sent ");
                        System.out.println("linda>");
                    } else {
                        out.print("VMDataNotAvailable");
                        out.flush();
                        System.out.println("Read request data Not available -- response sent ");
                        System.out.println("linda>");
                    }


                }

                else if (inputString.startsWith("hostRecoveryInfo")){
                    //System.out.println("host recovery information received "+inputString);
                    updateHostInfoFile(hostsFilePath,parsedArray[1],parsedArray[2],Integer.parseInt(parsedArray[3]));
                    serverDetails getDtls = connected_host_details.get(hostname_id_map.get(parsedArray[1]));
                    serverDetails updateDtls = new serverDetails(getDtls.getHostID(),parsedArray[1],parsedArray[2],Integer.parseInt(parsedArray[3]));
                    connected_host_details.put(hostname_id_map.get(parsedArray[1]),updateDtls);
                    out.print("updated");
                    out.flush();
                    int getBackupHostID = hostid_backuphostid.get(hostname_id_map.get(parsedArray[1]));
                    int getCurrentHostBackup = hostid_backuphostid.get(local_id);

                    if(getBackupHostID == local_id) { //if current system store backup data of recover host
                        System.out.println("current system store backup data of recover host");
                        sendBackupTupleData(hostname_id_map.get(parsedArray[1]));
                        sendMasterInformation(hostname_id_map.get(parsedArray[1]));
                        transferTopologyInformation(parsedArray[2],Integer.parseInt(parsedArray[3]));
                        System.out.println("Master host and topology information sent");
                    }

                    if(getCurrentHostBackup == hostname_id_map.get(parsedArray[1])){
                        System.out.println("Recover host store backup data of current host");
                        sendOrigTupleData(hostname_id_map.get(parsedArray[1]));
                    }
                    System.out.println("linda>");
                }
                else if (inputString.startsWith("removeTupleBackup")){
                    //System.out.println("removeTupleBackup received "+inputString);
                    String[] split = inputString.split("/");
                    removeTuple_bkp(split[1]);
                    removeTupleFromFile_bkp(absolutePathBackupTuple,split[1]);
                    out.print("removed");
                    out.flush();
                    System.out.println("linda>");
                }


            } catch (IOException e) {
                System.out.println("Read failed");
            }
            try {
                in.close();
                out.close();
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("ServerListner : main thread connection failed");
            } finally {
                if (in != null) {
                    in = null;
                }
                if (out != null) {
                    out = null;
                }
                if (socket != null) {
                    socket = null;
                }
            }

        }
    }
//Class end
}
