#!/bin/bash

# Root directory of your project
#PROJDIR=$HOME/AOS/Project1
#
# This assumes your config file is named "config.txt"
# and is located in your project directory
#
#CONFIG=$PROJDIR/config.txt
#
# Directory your java classes are in
#
#BINDIR=$PROJDIR/bin
#
# Your main project class
#
PROG=NodeServer
NETID=$2
PROGRAM_PATH=/home/013/r/rx/rxk152130/AOS_P2

#rm *.class *.out
#javac *.java


cat $1 | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    #echo $i    
    #netId=$( echo $i | awk '{ print $2 }' )
    totalNodes=$( echo $i | awk '{ print $1 }' )
    #echo $netId	
    
    for ((a=1; a <= $totalNodes ; a++))
    do
    	read line 
	#echo $line
	nodeId=$( echo $line | awk '{ print $1 }' )
       	host=$( echo $line | awk '{ print $2 }' )
        
	echo $nodeId
	echo $host
	echo $NETID
	echo $PROGRAM_PATH
	ssh -o StrictHostKeyChecking=no -l "$NETID" "$host.utdallas.edu" "killall -9 -u $NETID" &
	
    done
   
)
echo Cleanup Complete


