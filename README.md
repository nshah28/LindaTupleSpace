# LindaTupleSpace

Read ME

In this Project , Linda Tuple Space is a distributed Model has been implemented by using socket programming 
under multithreaded environment.

Files :

This project contains Three file

serverDetails.java
TopologyInfo.java
P1.java

Classes :
P2.class
P2.ServerListener.class
P2.commandPromptClient.class
TopologyInfo.class
serverDetails.class

Execution Steps:

-Create a directory and Copy the file  P2.java in a directory.
1) Compile the JAVA files using command and steps

Step-1 : javac serverDetails.java
Step-2 : javac TopologyInfo.java
Step-3 : javac P2.java


2) Execute the file using command
Java P2 <HostName>

<Host Name> is a unique host name of each host.

-Run the above command on multiple machines which are a part of distributed System.
-Add all hosts on one machine by using command and format:

add (host_name1,ip_address1,port_number1) (host_name2,ip_address2,port_number2)

This command will establish connection with all hosts and shares the details of all host information with each host in a distributed system.


-Delete Command syntax:

delete (host_1,host_2)

host_1 and hos_2 are the name of hosts which should be uniquely given at the time of executing P2 program.
Please Note that delete command will delete all the net,tuple files of host. But it will keep host_name folder as it is so that grader can cross check. Do not use the same name while running the program again except recovery case.
This command will forward the delete request to all hosts in system. Host which is going to be deleted would distribute and transfer all its tuple to other hosts and after that it will exit from the program.

-Recovery Case :
After crashing any host, execute the command

Command : java P2 host_name

Please use the same host_name which you have used before.



Useful Commands to work with Tuple space:
                                                                                                                                                                                       
OUT - OUT command will simply put tuple on a tuple space
IN - IN command will get the tuple from tuple space by removing it from tuple space
RD- RD command will get the tuple from tuple space without removing it from tuple space
Exact Data - Data String would contains some values of integer, string or float. (For eg. ((“abc”, 3))
Variable Data - Data String would contains either values or variable data. (For eg. ((“abc”, ?i:int,?s:string,?f:float)))

Example:

Out command :

Input command : out(“abc”, 3) output : put tuple (“abc”, 3) on 129.210.16.81

Input command : out(“abc”, 1) output : put tuple (“abc”, 3) on 129.210.16.82

RD command :

Input command :  rd(“abc”, 1)
Output: get tuple (“abc”, 1) on 129.210.16.81

Input command :  rd(“abc”, ?i:int)
Output: get tuple (“abc”, 1) on 129.210.16.81

IN command :

Input command :  in(“abc”, 1)
Output: get tuple (“abc”, 1) on 129.210.16.81

Input command :  in(“abc”, ?i:int)
Output: get tuple (“abc”, 1) on 129.210.16.81

