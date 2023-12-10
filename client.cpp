#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<string.h>
#include <iostream>
#include <vector>
#include <array>
#include <string>
#include<netdb.h>
#include<sys/socket.h>
#include<sys/types.h>
#include <assert.h>

using namespace std;
#define MAX 512


static uint32_t read_all(int fd, char *buf, size_t n){
    int read_res;

    while(n > 0){
        read_res = read(fd, buf, n);
//       printf("read_res is %d\n", read_res);
        if(read_res<= 0){
            return -1;
        }
        assert((size_t) read_res <= n);
        buf += read_res;
        n -= read_res;
        //printf("decremented n is %zu\n", n);
    }
//   printf("out of while in read all\n");
    return 0;
}

static uint32_t write_all(int fd, char *buf, size_t n){
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

static int32_t send_req(int fd, char *message){
    uint32_t len = (uint32_t) strlen(message);
//    printf("client: len of message is %u\n", len);
    if(len> MAX){
        return -1;
    }
    char w_buf[4 + MAX];
    memset(w_buf, 0, 4 + MAX);
    memcpy(w_buf, &len, 4);
        
    memcpy(&w_buf[4], message, len);
 /*
    for(int i=0; i<4+len; i++){
        printf("client: %x. %c\n", w_buf[i], w_buf[i]);
    }
 */

    int err = write_all(fd, w_buf, 4+len);
    if(err <0){
        return -1;
    }
    return 0;
}

static uint32_t read_res(int fd){
     uint32_t len;
    char r_buf[4 + MAX + 1];
    memset(r_buf, 0, 4 + MAX + 1);
    int err = read_all(fd, r_buf, 4);
    if(err<0){
        return -1;
    }

    memcpy(&len, r_buf,4);
    if(len >MAX){
        return -1;
    }
    err = read_all(fd, &r_buf[4], len);
    if(err<0){
        perror("read_all");
    }
    //r_buf[4+len]='\0';
    //printf("server says: %s\n", &r_buf[4]);
    return 0;
}

static int32_t send_req_cmd(int fd, vector <string> cmd){
    uint32_t len=4;
    for(string s: cmd){
        len+= 4+s.size();
    }
//    printf("total length : %u\n", len);
    if(len> MAX){
        return -1;
    }

    char wbuf[4+MAX];
    memcpy(&wbuf[0], &len, 4);
    uint32_t n = cmd.size();
    memcpy(&wbuf[4], &n, 4);
    size_t cur = 8;
    for(string &s: cmd){
        uint32_t p = (uint32_t) s.size();
        memcpy(&wbuf[cur], &p, 4);
        memcpy(&wbuf[cur + 4], s.data(), s.size());
        cur += 4+ s.size();
    }  
    return write_all(fd, wbuf, 4+len);
}


enum{
    SER_NIL = 0, //nullptr
    SER_ERR = 1, //ERROR
    SER_STR = 2, //string
    SER_INT = 3, //int
    SER_DBL = 4, //double
    SER_ARR = 5, //array
};


static int32_t on_response(uint8_t *data, size_t size){
//   printf("on_response\n");
    if(size <1){
       // printf("size is less than 1\n");
        perror("bad response");
        return -1;
    }
//    printf("before switch\n");
    switch(data[0]){
        case SER_NIL:
            printf("nil \n");
            return 1;
        case SER_ERR:
        //printf("ERR \n");
        if(size< 1+8){
            perror("bad response");
            return -1;
        }
        {
            int32_t code =0;
            uint32_t len = 0;
            memcpy(&code, &data[1],4);
            memcpy(&len, &data[5], 4);
            if(size < 1+8+len){
                perror("bad response");
                return -1;
            }
            printf("err %d %.*s", code, len, &data[9]);
            return 1+8+len;
        }
        case SER_STR:
        //printf("STR \n");
            if(size <1+4){
                perror("bad response");
                return -1;
            }
            {
                uint32_t len = 0;
                memcpy(&len, &data[1], 4);
                if(size< 1+4 + len){
                    perror("bad response");
                    return -1;
                }
                printf("str %.*s\n", len, &data[1+4]);
                return 1+4+len;
            }
        case SER_INT:
            //printf("INT \n");
            if(size <1+8){
                perror("bad response");
                return -1;
            }
            {
                int64_t val = 0;
                memcpy(&val, &data[1], 8);
                printf("int %ld\n", val);
                return 1+8;
            }
        case SER_DBL:
            if(size < 1+8){
                perror("bad response");
                return -1;
            }
            {
                double val = 0;
                memcpy(&val, &data[1], 8);
                printf("dbl %g\n", val);
                return 1+8;
            }
        case SER_ARR:
        //printf("ARR \n");
            if(size <1+4){
                perror("bad response");
                return -1;
            }
            {
                uint32_t len = 0;
                memcpy(&len, &data[1], 4);
                printf("arr len = %u \n", len);
                size_t arr_bytes = 1+4;
                for(uint32_t i=0; i<len; ++i){
                    int32_t rv = on_response(&data[arr_bytes], size-arr_bytes);
                    if(rv<0){
                        return rv;
                    }
                    arr_bytes += (size_t) rv;
                }
                printf("arr end\n");
                return (int32_t) arr_bytes;
            }
        default:
        //printf("DEFAULT \n");
            perror("bad response");
            return -1;
    }
}

static uint32_t read_res_cmd(int fd){
    uint32_t len;
    char r_buf[4 + MAX + 1];
    memset(r_buf, 0, 4 + MAX + 1);
    int err = read_all(fd, r_buf, 4);
    if(err<0){
        return -1;
    }
    
    cout <<endl;

    memcpy(&len, r_buf,4);
    if(len >MAX){
        return -1;
    }
    err = read_all(fd, &r_buf[4], len);
    if(err<0){
        perror("read_all");
        return -1;
    }
    size_t size_of_r_buf = sizeof(r_buf);

    //r_buf[4+len]='\0';
    //printf("server says : %s\n", &r_buf[8]);
    int32_t rv = on_response((uint8_t *)&r_buf[4], len);
//    printf("after on response\n");
    if(rv >0  && (uint32_t)rv != len){
        perror("bad response");
        rv = -1;
    }
    
    return rv;
}

int main(int argc, char *argv[]){
//   cout <<"size of argc is "<<argc<<endl;
    if(argc <3){
        perror("not enough arguments");
        return 0;
    }

//addrinfo
    int fd;
    struct addrinfo hints, *servinfo, *p;
    memset(&hints, 0, sizeof hints);
    hints.ai_family=AF_INET;
    hints.ai_socktype=SOCK_STREAM;
    

    fd=getaddrinfo(argv[1], "1111", &hints, &servinfo);
    if(fd== -1){
        perror("getaddrinfo");
    }
//socket and connect
    int socket_res;
    int connect_res;
    for(p=servinfo; p!=NULL;p=p->ai_next){
        socket_res= socket(p->ai_family, p->ai_socktype, 0);
        if (socket_res == -1){
            perror("socket");
            continue;
        }
        connect_res=connect(socket_res,p->ai_addr,p->ai_addrlen );
        if(connect_res== -1){
            perror("connect");
            continue;
        }
        break;
    }

//recv
    vector <string> cmd;
    for(int i=2; i<argc; ++i){
       // printf("argv[%d] is %s  ", i, argv[i]);
        cmd.push_back(argv[i]);
    }
    //cout <<endl;
    int32_t err = send_req_cmd(socket_res, cmd);
    if(err){
        goto DONE;
    }
     err = read_res_cmd(socket_res);
    if(err){
        goto DONE;
    }

//close
DONE:
    close(socket_res);
    return 0;      
}