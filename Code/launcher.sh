#!/bin/bash

PROG=NodeServer #The Name of the Main Java Class
NETID=$2 #Net-ID from command line
PROGRAM_PATH=/home/013/r/rx/rxk152130/AOS_P2_Final #Location of class files in server

cat $1 | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    totalNodes=$( echo $i | awk '{ print $1 }' )
    
    for ((a=1; a <= $totalNodes ; a++))
    do
    	read line
	nodeId=$( echo $line | awk '{ print $1 }' )
       	host=$( echo $line | awk '{ print $2 }' )
        
	echo $nodeId
	echo $host
	echo $NETID
	echo $PROGRAM_PATH
	ssh -o StrictHostKeyChecking=no -l "$NETID" "$host.utdallas.edu" "cd $PROGRAM_PATH;java $PROG $nodeId $1" &
    done
)
echo Launch Complete