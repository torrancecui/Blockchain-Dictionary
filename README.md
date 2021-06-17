# Blockchain-Dictionary

A simplified key value store backed by a blockchain. Created by Torrance Cui in March 2021.

The program can support up to 5 different servers and up to 3 clients. Consensus among the 5 severs is reached through Multipaxos.

Starting a server from root folder:
```` 
//start up server with process id 1
python3 server.py 1
```` 

Starting a client from root folder:
```` 
//start up client with id 1
python3 client.py 1
```` 

Supported commands:
```` 
connect
````
Connects server or client to other running nodes.
```` 
put <key> <value>
```` 
Inputs key/value pair into the dictionary.
```` 
get <key>
```` 
Retreives value of given key from dictionary.
```` 
failLink <src> <dest>
```` 
Simulates a link failure between two server nodes.
```` 
fixLink <src> <dest>
```` 
Fixes the link failure between two server nodes.
