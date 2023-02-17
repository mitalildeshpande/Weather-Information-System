# Weather-Information-System

Developed a web application for Weather Information using Flask and MangoDB database.
The website takes in City name as input and posts it into the MangoDB database. This data stored in MangoDb database can be retrieved using a 
button provided (List Weather Info). Docker compose is used to build and link Flask and MangoDB containers from Docker file.

In the project, we have created five identical nodes and have selected one as a Leader node by conducting an election by looking out for heartbeats, 
timeouts, and remote-procedure-calls oneach node.

RAFT consensus protocol is used for thr LEADER selection.

When a node is elected as a leader node, all the other nodes will be its follower. So, the states possible here are Leader, Candidate and Follower.
Whenever a leader gets a request to store an entry into its logs it will gradually pass these entries to its followers through its heartbeats.

Multiple asynchronous threads running on each 5 nodes perform tasks like listening for incoming requests, sending heartbeats, processing packets
on receiver side, vote requests, etc.
