
# Redis


A Redis-like server implementation.

I used an article “build your own redis server with c/c++”. This server uses  data structures like hash table, AVL- tree, heap, linked list and queue.

https://build-your-own.org/redis/#table-of-contents

## Commands Supported and usage


1. "set" command sets a key-value data into the database.
    ./client <IP> <set> <key> <value>
2. "get" command gets a value of a given key from the database.
    ./client <IP> <get> <key>
3. "del" command deletes a key-value data into the database.
    ./client <IP> <del> <key>
4. "keys" command prints all the key present in the database.
    ./client <IP> <keys>
5. "zadd" command add a (score, name) pair to the database.
    ./client <IP> <zadd> <key> <score> <name>
6. "zrem" command removes a (score, name) pair from the database.
    ./client <IP> <zrem> <key> <name>
7. "zscore" command prints the score corresponding to the given name.
    ./client <IP> <zscore> <key> <name>
8. "zquery" command prints in sorted order the (score, name) data relative to the offset till it reaches a limit.
    ./client <IP> <zquery> <key> <score> <name> <offset> <limit>
9. "expire" command sets the time to live for the data in the database.
    ./client <IP> <expire> <key> <time_value> 
10. "ttl" command current "time to live" value for the data.
    ./client <IP> <ttl> <key>








   

