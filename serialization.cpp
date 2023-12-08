#include "header.h"


void out_nil(string &out){
    out.push_back(SER_NIL);
}

void out_dbl(std::string &out, double val) {
    out.push_back(SER_DBL);
    out.append((char *)&val, 8);
}

void out_str(string &out, const char *s, size_t size){
    //printf("into out_str\n");
    out.push_back(SER_STR);
    uint32_t len = (uint32_t)size;
    char *c= (char *) &len;
    out.append((char *)&len, 4);
    out.append(s, len);
    //printf("size of out after append(val) %d\n",out.size());
}

void out_str(string &out, string &val){
    return out_str(out, val.data(), val.size());
}

void out_int(string &out, int64_t val){
    out.push_back(SER_INT);
    out.append((char *)&val, 8);
}

void out_err(string &out, uint32_t code, string &msg){
    out.push_back(SER_ERR);
    out.append((char *)&code, 4);
    uint32_t len = (uint32_t)msg.size();
    out.append((char *)&len, 4);
    out.append(msg);
 /*
    cout <<"out message is :";
    for(int i=0; i<out.size(); i++){
        cout<<out[i]<<". ";
    }
    cout <<endl;
*/
}

void out_arr(string &out, uint32_t n){
    out.push_back(SER_ARR);
    out.append((char *)&n, 4);
    //printf("size of out after appends %d\n",out.size());
}

void cb_scan(HNode * node, void *arg){
    //printf("into cb_scan\n");
    string &out = *(string *)arg;
    out_str(out, container_of(node, struct Entry, node)->key);
}

void h_scan(Htable *htable, void (*f)(HNode *, void *), void *arg){
    //printf("h_scan:\n");
    if(htable->size == 0){
        //printf("size of hashtable is zero\n");
        return;
    }

    for(int i=0; i<(htable->mask+1); ++i){
        HNode *node = htable->h_arr[i];
        while(node){
            f(node, arg);
            node = node->next;
        }
    }
}


void do_keys_s(vector<string> &cmd, string &out){
    //printf("do_keys_s:\n");
    (void)cmd;
    out_arr(out, (uint32_t) hm_size(&d_data.db));
    h_scan(&d_data.db.ht1, &cb_scan, &out);
    h_scan(&d_data.db.ht2, &cb_scan, &out);
}

void do_set_s(vector <string> &cmd, string &out){
    //printf("do_set_s\n");
    (void) cmd;
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hcode = str_hash((uint8_t *) entry.key.data(), entry.key.size());
    //cout <<"do_set_s: addr is "<<&d_data.db<<endl;
    HNode *node = hm_lookup(&d_data.db, &entry.node, &entry_eq);
    if(node){
       //printf("there is already such a node\n");
        container_of(node, Entry, node)->value.swap(cmd[2]);
    }
    else{
        //printf("there is no such node\n");
        Entry *ent = new Entry();
        ent->key.swap(entry.key);
        ent->node.hcode = entry.node.hcode;
        ent->value.swap(cmd[2]);
        hm_insert(&d_data.db, &ent->node);
    }
    //printf("size of do_set_s is %d\n", out.size());
    return out_nil(out);
}


void do_del_s(vector<string> &cmd, string &out){
     //printf("do_del_s\n");
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hcode = str_hash((uint8_t *) entry.key.data(), entry.key.size());
    //cout <<entry.node.hcode<<endl;
    HNode *node = hm_pop(&d_data.db, &entry.node, &entry_eq);
    if(node){
        delete container_of(node, Entry, node);
    }
    return out_int(out, node? 1: 0);
}

void do_get_s(vector<string> &cmd, string &out){
    //printf("do_get_s\n");
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hcode = str_hash((uint8_t *) entry.key.data(), entry.key.size());
    //cout <<entry.node.hcode<<endl;
    HNode *node = hm_lookup(&d_data.db, &entry.node, &entry_eq);
    if(!node){
        //printf("found a node\n");
       return out_nil(out);
    }
    string &val = container_of(node, Entry, node)->value;
    return out_str(out, val);
}


void do_zadd(vector <string> &cmd, string &out){
    //cout <<"do_zadd: in"<<endl;
//convert score to double
    double score = 0;
    string msg = "do_zadd: expect fp number";
    if(!str2dbl(cmd[2], score)){
        return out_err(out, ERR_ARG, msg);
    }

//create new Entry. fill in fields
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hcode = str_hash((uint8_t *)entry.key.data(), entry.key.size());

//hm_lookup of hnode with give key
    HNode *hnode = hm_lookup(&d_data.db, &entry.node, &entry_eq);

//if found - find the container of hnode and chech its type
//if not found - create new Entry fill in fields. hm_insert
    Entry *ent= NULL;
    if(!hnode){
        ent = new Entry;
        ent->key.swap(entry.key);
        ent->node.hcode = entry.node.hcode;
        ent->type = T_ZSET;
        ent->zset = new ZSet();
        hm_insert(&d_data.db, &ent->node);
    }else{
        ent = container_of(hnode, Entry, node);
        msg = "expect zset";
        if(ent->type != T_ZSET){
            return out_err(out, ERR_TYPE, msg);
        }
    }

//add name and score to zset
        const string &name = cmd[3];
        bool added = zset_add(ent->zset, name.data(), name.size(), score);
/*
        cout <<" do_zadd: out is ";
        for(int i=0 ; i<out.size(); i++){
            cout <<out[i]<<" ";
        }
        cout<<endl;
*/
        return out_int(out, (int64_t) added);
}

bool expect_zset(string &out, string &s, Entry **ent){
    Entry entry;
    entry.key.swap(s);
    entry.node.hcode = str_hash((uint8_t *)entry.key.data(), entry.key.size());
    HNode *hnode = hm_lookup(&d_data.db, &entry.node, &entry_eq);
   // cout <<"expect_zset: hnode from hm_lookup with key "<<hnode<<endl;
    if(!hnode){
        out_nil(out);
        return false;
    }
    *ent = container_of(hnode, Entry, node);
    string msg = "expect zset";
    if((*ent)->type != T_ZSET){
        //cout <<"type is not t_zset\n";
        out_err(out, ERR_TYPE, msg);
        return false;
    }
    return true;
}

void do_zrem(vector <string> &cmd, string &out){
    //cout <<"do_zrem: \n";
    Entry *entry = NULL;
    if(!expect_zset(out, cmd[1], &entry)){
        return;
    }

    //cout <<"do_zrem: there is a entry with this key"<< entry<<endl;;
    string &name = cmd[2];
    ZNode *znode = zset_pop(entry->zset, name.data(), name.size());
    //cout <<"return from zset_pop : "<<znode<<endl;
    if(znode){
        znode_del(znode);
    }
    return out_int(out, znode? 1 : 0);
}

//is there a zset with given score
void do_zscore(vector <string> &cmd, string &out){
    Entry *ent = NULL;
    if(!expect_zset(out, cmd[1], &ent)){
        return;
    }
    string &name = cmd[2];
    ZNode *znode = zset_lookup(ent->zset, name.data(), name.size());
    return znode? out_dbl(out, znode->score) : out_nil(out);
}

void out_update_arr(string &out, uint32_t n){
    assert(out[0] == SER_ARR);
    memcpy(&out[1], &n, 4);
/*
    cout <<"len of out is "<<out.size()<<endl;
        cout <<" out_update_arr: out is ";
        for(int i=0 ; i<out.size(); i++){
            cout <<out[i]<<" ";
        }
        cout<<endl;
*/
}

void do_zquery(vector <string> &cmd, string &out){
    string msg = "expect fp number";
    double score = 0;
    if(!str2dbl(cmd[2], score)){
        return out_err(out, ERR_ARG, msg);
    }
    string &name = cmd[3];
    int64_t offset =0;
    int64_t limit =0;
    string msg1 = "expect int";
    if(!str2int(cmd[4], offset)){
        return out_err(out, ERR_ARG, msg1);
    }
    
    if(!str2int(cmd[5], limit)){
        return out_err(out, ERR_ARG, msg1);
    }
    //cout <<"out is "<<out<<endl;
    Entry *ent = NULL;
    if(!expect_zset(out, cmd[1], &ent)){
        //cout <<"zset not added already\n";
        if(out[0] == SER_NIL){
            out.clear();
            out_arr(out, 0);
        }
        return;
    }

    if(limit <=0){
        //cout <<"limit is <= 0\n";
        return out_arr(out, 0);
    }
    ZNode *znode = zset_query(ent->zset, score, name.data(), name.size(), offset);
    //cout <<"res of zset_query"<<znode<<endl;

    out_arr(out, 0);
    uint32_t n = 0;
    //cout <<"n and limit is "<<n<<" "<<limit<<endl;
    while(znode && (int64_t)n < limit){
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = container_of(avl_offset(&znode->tree, 1), ZNode, tree);
        n += 2;
        //cout <<"n is changed to "<<n<<endl;
    }
    return out_update_arr(out, n);
}


void do_ttl(vector<string> &cmd, string &out){
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hcode = str_hash((uint8_t *)entry.key.data(), entry.key.size());
    //cout <<"do_ttl: addr is "<<&d_data.db<<endl;
    HNode *node = hm_lookup(&d_data.db, &entry.node, &entry_eq);
    //cout <<"return rom hm_lookup\n";
    if(!node){
        //cout <<"do_ttl: null return\n";
        return out_int(out, -1);
    }
    Entry *ent = container_of(node, Entry, node);
    if(ent->heap_idx == (size_t)-1){
        //cout <<"do_ttl: heap idx is -1\n";
        return out_int(out, -1);
    }
    //cout<<"heap size "<<d_data.heap.size();
    uint64_t expire_time = d_data.heap[ent->heap_idx].val;
    uint64_t now_time = get_monotonic_usec();
    return out_int(out, expire_time > now_time ? (expire_time - now_time)/1000 : 0);
}

void entry_set_ttl(Entry *ent, int64_t ttl_ms){
    //cout <<"entry_set_ttl: ttl_ms "<<ttl_ms<<endl;
    if(ttl_ms < 0 && ent->heap_idx != (size_t)-1){
        size_t p = ent->heap_idx;
        d_data.heap[p] = d_data.heap.back();
        //cout <<"timer at idx "<<p<<" is "<<d_data.heap[p].val<<endl;
        d_data.heap.pop_back();
        if(p< d_data.heap.size()){
            heap_update(d_data.heap.data(), p, d_data.heap.size());
            //cout <<"heap size is "<<d_data.heap.size()<<endl;
        } 
        //cout <<"after update timer at idx "<<p<<" is "<<d_data.heap[p].val<<endl;
        ent->heap_idx = -1;
        
    }else if(ttl_ms >= 0){
        size_t p = ent->heap_idx;
        if(p == (size_t)-1){
            HeapItem item;
            item.ref = &ent->heap_idx;
            d_data.heap.push_back(item);
            p = d_data.heap.size()-1;
        }
        d_data.heap[p].val = get_monotonic_usec() + (uint64_t) ttl_ms *1000;
        heap_update(d_data.heap.data(), p, d_data.heap.size());
        //cout <<"heap size is "<<d_data.heap.size()<<endl;
/*
        cout <<"elts in heap: ";
        for(int i=0; i<d_data.heap.size(); i++){
            cout <<d_data.heap[p].val<<" ";
        }
        cout <<endl;
*/      
    }
}

void do_expire(vector<string> &cmd, string &out){
    //cout <<"do_expire: \n";
    int64_t ttl_ms =0;
    if(!str2int(cmd[2], ttl_ms)){
        string msg = "expect int64";
        return out_err(out, ERR_ARG, msg);
    }

    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hcode = str_hash((uint8_t *) entry.key.data(), entry.key.size());

    HNode *node = hm_lookup(&d_data.db, &entry.node, &entry_eq);
    //cout<<"do_expire: return from hm_lookup is "<<node<<endl;
    if(node){
        //cout <<"do_expire: node is there\n";
        Entry *ent = container_of(node, Entry, node);
        entry_set_ttl(ent, ttl_ms);
    }
    return out_int(out, node ? 1:0);
}

void do_request_serial(vector <string> &cmd, string &out){
    if(cmd.size() == 1 && cmd_is(cmd[0], "keys")){
       // printf("its keys\n");
        do_keys_s(cmd, out);
    }else if(cmd.size() == 2 && cmd_is(cmd[0], "get")){
        do_get_s(cmd, out);
    }else if(cmd.size() == 3 && cmd_is(cmd[0], "set")){
        do_set_s(cmd, out);
    }else if(cmd.size() == 2 && cmd_is(cmd[0], "del")){
        do_del_s(cmd, out);
    }else if(cmd.size() == 4 && cmd_is(cmd[0], "zadd")){
        do_zadd(cmd, out);
    }else if(cmd.size() == 3 && cmd_is(cmd[0], "zrem")){
        do_zrem(cmd, out);
    }else if(cmd.size() == 3 && cmd_is(cmd[0], "zscore")){
        do_zscore(cmd, out);
    }else if(cmd.size() == 6 && cmd_is(cmd[0], "zquery")){
        do_zquery(cmd, out);
    }else if(cmd.size() == 3 && cmd_is(cmd[0], "expire")) {
        do_expire(cmd, out);
    } else if(cmd.size()==2 &&cmd_is(cmd[0], "ttl")) {
        do_ttl(cmd, out);
    } else{
        string msg = "unknown cmd";
        out_err(out, ERR_UNKNOWN, msg);
    }
}


bool try_one_request_serial(Con *con){
     //printf("try_new_request_serial\n");

    uint32_t len = 0;//number of strings
    memcpy(&len, &con->r_buf[0], 4);
     //printf("total length : %u\n", len);

    if(4+len > con->rbuf_size){
        return 0;
    } 
    vector<string> cmd;
    int parse_request_res = parse_request((uint8_t *)&con->r_buf[4], len, cmd);
    if(parse_request_res !=0){
        perror("parse_request");
        return 0;
    }


    string out;
    do_request_serial(cmd, out);

    if(4+out.size() > MAX){
       // printf("4+out size is greater than max\n");
        out.clear();
        string msg1 = "response too big";
        out_err(out, ERR_2BIG, msg1);
    }
    uint32_t wlen = (uint32_t) out.size();
    //printf("out size is %u\n", wlen);
    memcpy(&con->w_buf[0], &wlen,4);
    memcpy(&con->w_buf[4], out.data(), out.size());
    con->wbuf_size = 4+wlen;
    size_t remain = con->rbuf_size - 4 - len;
    //printf("remain is %d\n", remain);
    if(remain){
        memmove (con->r_buf, &con->r_buf[4+len], remain);
    }
    con->rbuf_size= remain;

    con->state = STATE_RES;
    state_res(con);

    return (con->state == STATE_REQ);

}