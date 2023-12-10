
#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <inttypes.h>
#include<sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h> 
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <vector>
#include <map>
#include "header.h"

using namespace std;
#define MAX 512

enum{
    RES_OK = 0, RES_ERR = 1, RES_NX = 2
};

//a global hashmap
static map<string, string> str_map;
bool try_one_request_serial(Con *);
void thread_pool_init(ThreadPool *, size_t);
ddata d_data;

//initialse Dlist with null
void dlist_init(DList *node){
    node->prev = node->next = node;
}

//if next node is the node itself the dlist is empty

void dlist_detach(DList *node){
    DList *prev = node->prev;
    DList *next = node->next;

    prev->next = next;
    next->prev = prev;
}

//add a new_node before target
void dlist_insert( DList *target, DList *new_node){
    DList *prev = target->prev;

    prev->next = new_node;
    new_node->prev = prev;

    new_node->next = target;
    target->prev = new_node;
}

void con_done(Con *con){
    d_data.con[con->fd] = NULL;
    close(con->fd);
    dlist_detach(&con->idle_list);
    free(con);
}

static void fd_set_nonBlocking(int fd){
    //cout <<"fd_set_nonBlocking:\n";
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if(errno){
        perror("fcntl");
        return ;
    }
    flags |= O_NONBLOCK;
    //printf("flags after non bocking is %d \n", flags);
    errno =0;
    fcntl(fd, F_SETFL, flags);
    if(errno){
        perror("fcntl");
    }
}

static uint32_t read_all(int fd, char *buf, size_t n){
    //printf("read_all\n");
    int read_res;

    while(n > 0){
        read_res = read(fd, buf, n);
        if(read_res< 0){
            return -1;
        }
        buf += read_res;
        n -= read_res;
      //  printf("decremented n is %zu\n", n);
    }
    return 0;
}

static uint32_t write_all(int fd, char *buf, size_t n){
   // printf("write_all\n");
    int write_res;
    while(n){
        write_res = write(fd, buf, n);
        if(write_res <0){
            return -1;
        }
        buf += write_res;
        n -= write_res;
    }
    return 0;
}

//if the size fo connects vector is less that fd
static void conn_put(vector<Con *> &connects, struct Con* con){
    //printf("conn_put\n");

    if(connects.size() <= (size_t)con->fd){
        connects.resize((con->fd)+1);
    }
    connects[con->fd] = con;
}

static void accept_new_con(vector <Con *> &connects, int fd){
    //printf("accept_new_con\n");
    int accept_res;
    struct sockaddr_storage their_addr;
    socklen_t len = sizeof their_addr;
    accept_res = accept(fd, (struct sockaddr *)&their_addr, &len);
    if(accept_res == - 1){
        perror("accept");
        return;
    }
    //set new fd in non-blocking mode
    fd_set_nonBlocking(accept_res);

    struct Con *con = (struct Con *) malloc(sizeof(struct Con));
    con->fd = accept_res;
    con->state = STATE_REQ;
    con->rbuf_size = 0;
    con->wbuf_send=0;
    con->wbuf_size=0;
    con->idle_start = get_monotonic_usec();
    dlist_insert(&d_data.idle_list, &con->idle_list);
    conn_put(connects, con);
    return;
}

static bool try_flush_buffer(Con *con){
    //printf("try_flush_buffer\n");
    ssize_t rv = 0;

    do{
        size_t remain = con->wbuf_size - con->wbuf_send;
        rv = write(con->fd, &con->w_buf[con->wbuf_send], remain);
    }while(rv<0 && errno == EAGAIN);

    if(rv<0 && errno == EAGAIN){
        return 0;
    }

    if(rv<0){
        perror("write");
        return 0;
    }

    con->wbuf_send += (size_t)rv;
    if(con->wbuf_send == con->wbuf_size){
        con->state = STATE_REQ;
        con->wbuf_send = 0;
        con->wbuf_size = 0;
        return 0;
    }

    return 1;
}

void state_res(Con *con){
    //printf("state_res\n");
    while(try_flush_buffer(con)){}
}

static bool try_one_request(Con *con){
    //printf("try_one_request\n");

    //parse a request
    uint32_t len = 0;
        
    memcpy(&len, &(con->r_buf[0]), 4);
    //printf("length is %u\n", len);
    if(4+len > con->rbuf_size){
        return 0;
    } 

    //printf("client says: %s\n", &con->r_buf[4]);

    memset(&con->w_buf, 0, 4+MAX);
    memcpy(&con->w_buf[0], &len, 4);
    memcpy(&con->w_buf[4], &con->r_buf[4], len);
    con->wbuf_size = 4+ len;

    size_t remain = con->rbuf_size - 4 - len;
    if(remain){
        memmove (con->r_buf, &con->r_buf[4+len], remain);
    }
    con->rbuf_size= remain;

    con->state = STATE_RES;
    state_res(con);

    return (con->state == STATE_REQ);
}

//fill cmd vector from the read buffer
int32_t parse_request(uint8_t *in, uint32_t reqlen, vector <string> &cmd){
    //printf("parse_ requst\n");

    uint32_t no_of_str =0; //no of strings
    memcpy(&no_of_str,&in[0], 4);//copy first four - gives no of strings

    size_t p = 4;//start from 4th idx of in
    while(no_of_str--){
        if(reqlen < p +4){
            return -1;
        }
        uint32_t str_size =0;
        memcpy(&str_size, &in[p], 4);

        if(reqlen < 4+p+str_size){
            return -1;
        }
        cmd.push_back(string ((char *)&in[p+4], str_size));
        p += 4+ str_size;
    }
    if(p != reqlen){
        return -1;
    }
    return 0;
}

int32_t cmd_is(string a, string b){
    if(a==b){
        //cout <<"cmd_is: "<<a<<endl;
        return 1;
    }
    return 0;
}

static bool fill_buffer(Con *con){
    //printf("fill_buffer\n");
    ssize_t rv = 0;
    do{
        size_t cap = sizeof(con->r_buf)- con->rbuf_size;
        rv = read(con->fd, &con->r_buf[con->rbuf_size], cap);
    }while(rv<0 && errno == EINTR);

    if(rv<0 && errno == EAGAIN){
        return 0;
    }

    if(rv<0){
        perror("read");
        return 0;
    }
    if(rv == 0){
        if(con->rbuf_size> 0){
            perror("unexpected EOF");
        }else{
            perror("EOF");
        }
        con->state = STATE_END;
        return 0;
    }

    con->rbuf_size += (size_t)rv;
    while(try_one_request_serial(con)){}
    return (con->state == STATE_REQ);
}

static void state_req(Con *con){
    while(fill_buffer(con)){}
}

static void connection_io(Con *con){
    //printf("connection_io\n");
    con->idle_start = get_monotonic_usec();
    dlist_detach(&con->idle_list);
    dlist_insert(&d_data.idle_list, &con->idle_list);

    if(con->state == STATE_REQ){
        state_req(con);
    }else if(con->state == STATE_RES){
        state_res(con);
    }else{
        return;
    }
}

uint32_t next_timer_ms(){
    //cout <<"next_timer_ms: in\n"<<endl;

    //get_monotonic_usec() return time is microseconds
    uint64_t now_ms = get_monotonic_usec();
    uint64_t next_ms = (uint64_t) -1;
    //cout <<d_data.heap[0].val <<endl;
    //idle timers

    if(!dlist_empty(&d_data.idle_list)){       
        Con *next = container_of(d_data.idle_list.next, Con, idle_list);

       //k_idle_timeout_ms *1000- covert milliseconds to microseconds
        next_ms = next->idle_start + k_idle_timeout_ms *1000;
    }
    //ttl timers
    if(!d_data.heap.empty() && d_data.heap[0].val < next_ms){
        next_ms = d_data.heap[0].val;
    }
    if(next_ms == (uint64_t) -1){
        return 10000;
    }
    if(next_ms <= now_ms){
        //missed it
        return 0;
    }

    //we return dividing to 1000 to get results in microseconds
    return (uint32_t)((next_ms - now_ms)/1000);
}

static bool hnode_same(HNode *lhs, HNode *rhs) {
    return lhs == rhs;
}

void process_timers(){
    //cout <<"process_timers: \n";
    uint64_t now_us = get_monotonic_usec() + 1000;

    //idle itmers
    while(!dlist_empty(&d_data.idle_list)){
        //cout <<"if list is not empty \n";
        Con *next = container_of(d_data.idle_list.next, Con, idle_list);
        uint64_t next_us = next->idle_start + k_idle_timeout_ms *1000;
       // cout << "next_us :"<<next_us<<endl;
        if(next_us >= now_us +1000){
            break;
        }
        //printf("process_timers: romoving ide connection: %d\n", next->fd);
        con_done(next);
    }

    //ttl timers
    size_t k_max_works = 2000;
    size_t nworks = 0;

    //cout <<"process_timers: heap is empty "<< d_data.heap.empty()<<endl;
    while(!d_data.heap.empty() && d_data.heap[0].val <now_us){

        Entry *ent = container_of(d_data.heap[0].ref, Entry, heap_idx);

        HNode *node = hm_pop(&d_data.db, &ent->node, &hnode_same);
        //cout <<"detail: "<<ent->key<<endl;
       // cout <<"detail: "<<ent->type<<endl;       
        if(ent->zset){
        //cout <<"detail: "<<ent->zset->hmap.ht1.size+ent->zset->hmap.ht2.size<<endl;
        }

        assert(node == &ent->node);
        entry_del(ent);

        if(nworks++ >= k_max_works){
            break;
        }
    }
    //cout<<"process_timer: out\n";
}

int main(){
   //("main\n");
//addrinfo  
    struct addrinfo hints;//i come from netdb
    struct addrinfo *getaddrinfo_res, *p;
    memset(&hints, 0, sizeof hints);

    hints.ai_flags= AI_PASSIVE;
    hints.ai_family=AF_INET;
    hints.ai_socktype=SOCK_STREAM;

    //use getaddrinfo to fill the rest of the fields of addrinfo
    int result = getaddrinfo(NULL, "1111", &hints,  &getaddrinfo_res);
    if(result != 0){
        fprintf(stderr, "getaddrinfo : %s\n", gai_strerror(result));
    }
    //printf("listening at port 1111\n");
    //printf("resuts from getaddrinfo is %d \n", result);



//socket & bind
    int socket_res;
    //returns a socket descriptor
    int bind_res;
    for(p=getaddrinfo_res; p != NULL; p=p->ai_next){
        socket_res= socket(p->ai_family, p->ai_socktype, 0);
        //printf("resuts from socket is %d \n", socket_res);
        if(socket_res==-1){
            perror("socket");
            continue;           
        }

        bind_res = ::bind(socket_res, p->ai_addr, p->ai_addrlen);
        //printf("resuts from bind is %d \n", bind_res);
        if(bind_res==-1){
            perror("bind");
            continue;
        }
        break;
    }
    if(p==NULL){
        perror("failed to bind");
    }
    //cout <<"got listenter fd\n";

//listen
    int listen_res;
    listen_res= listen(socket_res, 10);
    if(listen_res==-1){
        perror("listen");
        return 1;
    }

    //cout <<" listening \n";

    fd_set_nonBlocking(socket_res);

    dlist_init(&d_data.idle_list);

    //cout <<"create initialse thread pool\n";
    thread_pool_init(&d_data.tp, 4);

    vector <struct pollfd> pfds;

    while(1){
        //prepare arguments for poll
        pfds.clear();

        //listening fd is put in first
        //cout <<" create listener fd \n";
        struct pollfd pfd = {socket_res, POLLIN, 0};
        pfds.push_back(pfd);

        for(Con *con : d_data.con){
            //cout <<"con is : "<<con<<endl;
            if(!con){   //if con is 0 continue
            //cout <<"con id empty\n";
                continue;
            }
            struct pollfd pfd= {0, POLLIN, 0};
             
            pfd.fd = con->fd;
            pfd.events=(con->state == STATE_REQ)? POLLIN : POLLOUT;
            pfd.events = pfd.events | POLLERR;
            pfds.push_back(pfd);
        }


        int timeout_ms = (int) next_timer_ms();
        //cout << "timeout_ms is "<<timeout_ms<<endl;
//poll for active fds
        int rv = poll(pfds.data(), (nfds_t) pfds.size(), timeout_ms);
        if(rv <0){
            perror("poll");
        }

        for(size_t i =1; i< pfds.size(); ++i){
            //cout <<"for i is "<<i<<endl;
            if(pfds[i].revents){
                Con *con = d_data.con[pfds[i].fd];
                connection_io(con);
                if(con->state == STATE_END){
                    //cout <<"state of connection is end\n";
                    con_done(con);
                }
            }
        }
        //go through the heap and remove timeout connections
        process_timers();

        if(pfds[0].revents){
            //cout <<"it is a listener\n";
            accept_new_con(d_data.con,socket_res);
        }//end if        
    }//end while
    return 0;
}//endl main



/*
//test in terminal
 nc -v -n ip 1112
*/

