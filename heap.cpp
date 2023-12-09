#include "header.h"


//returns idx of parent
size_t heap_parent(size_t i){
    return (i+1)/ 2-1;
}

//returns idx of left child
size_t heap_left(size_t i){
    return i * 2 +1;
}

//returns idx of right child
size_t heap_right(size_t i){
    return i * 2 +2;
}

void heap_up(HeapItem *a, size_t p){
    //cout <<"heap_up"<<endl;
    HeapItem t = a[p];
    while (p >0 && a[heap_parent(p)].val > t.val){
        a[p] = a[heap_parent(p)];
        *a[p].ref = p;
        p = heap_parent(p);
    }
    a[p]= t;
    *a[p].ref = p;
    //cout << "idx is changed to "<<p<<endl;
}

void heap_down(HeapItem *a, size_t p, size_t len){
    //cout <<"heap_down"<<endl;
    HeapItem t = a[p];
    while(true){
        size_t l = heap_left(p);
        size_t r = heap_right(p);
        //cout <<"left and right is "<<l <<" "<<r<<endl;
        size_t min_pos = -1;
        size_t min_val = t.val;
        if(l < len && a[l].val < min_val){
            //cout << " left is less than min val"<<endl;
            min_pos = l;
            min_val = a[l].val;
        }
        if(r<len && a[r].val < min_val){
            //cout << " right is less than min val"<<endl;
            min_pos = r;
        }
        if(min_pos == (size_t)-1){
            //cout <<"min is -1"<<endl;
            break;
        }
        a[p] = a[min_pos];
        *a[p].ref = p;
        p = min_pos;
    }
    a[p] = t;
    *a[p].ref = p;
}

void heap_update(HeapItem *a, size_t p, size_t len){
   //cout <<"heap_update:"<<endl;
    if(p > 0 && a[heap_parent(p)].val > a[p].val){
        //cout <<"gonna heap up"<<endl;
        heap_up(a, p);
    }else{
        //cout <<"gonna heap down"<<endl;
        heap_down(a, p, len);     
    }
}


//if next node is the node itself the dlist is empty
bool dlist_empty( DList *node){
    return node->next == node;
}

uint64_t get_monotonic_usec() {
    timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return uint64_t(tv.tv_sec) * 1000000 + tv.tv_nsec / 1000;
}

struct h_data {
    size_t heap_idx = -1;
};

struct Container {
    std::vector<HeapItem> heap;
    std::multimap<uint64_t, h_data *> map;
};

static void heap_dispose(Container &c) {
    for (auto p : c.map) {
        delete p.second;
    }
}

static void heap_add(Container &c, uint64_t val) {
    //cout << "heap_add: "<<endl;
    h_data *d = new h_data();
    c.map.insert(std::make_pair(val, d));
    HeapItem item;
    item.ref = &d->heap_idx;
    item.val = val;
    c.heap.push_back(item);

/*
    cout <<"data in c's heap"<<endl;
    for(int i=0 ;i< c.heap.size() ; i++){
        cout << c.heap[i].ref <<":"<< c.heap[i].val<<" ";
    }
    cout<<endl;
*/

    heap_update(c.heap.data(), c.heap.size() - 1, c.heap.size());
}

static void heap_del(Container &c, uint64_t val) {
    //cout <<"heap_Del: "<<endl;
    auto it = c.map.find(val);

    assert(it != c.map.end());
    h_data *d = it->second;

    assert(c.heap.at(d->heap_idx).val == val);
    assert(c.heap.at(d->heap_idx).ref == &d->heap_idx);
    c.heap[d->heap_idx] = c.heap.back();

    c.heap.pop_back();
    if (d->heap_idx < c.heap.size()) {
        heap_update(c.heap.data(), d->heap_idx, c.heap.size());
    }
    delete d;
    c.map.erase(it);
}

uint32_t next_timer_ms_mod(){
    uint64_t now_ms = get_monotonic_usec();
    uint64_t next_ms = (uint64_t) -1;

    //idle timers
    if(!dlist_empty(&d_data.idle_list)){
        Con *next = container_of(d_data.idle_list.next, Con, idle_list);
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
    return (uint32_t)((next_ms - now_ms)/1000);
}

void tree_dispose(AVLNode *node) {
    if (!node) {
        return;
    }
    tree_dispose(node->left);
    tree_dispose(node->right);
    znode_del(container_of(node, ZNode, tree));
}


// destroy the zset
void zset_dispose(ZSet *zset) {
    tree_dispose(zset->tree);
    hm_destroy(&zset->hmap);
}

void entry_destroy(Entry *ent){
    //cout <<"entry_destroy: \n";
    switch(ent->type){
        case T_ZSET:
            zset_dispose(ent->zset);
            delete ent->zset;
            break;
    }
    entry_set_ttl(ent, -1);
    //cout <<"ent destroy "<<ent->key<<endl;
    delete ent;
}

void entry_del_async(void *arg){
    entry_destroy((Entry *)arg);
}

size_t hm_size(HMap *hmap) {
    return hmap->ht1.size + hmap->ht2.size;
}

void entry_del(Entry *ent){
    //cout<<"entry_del: in\n";
    
    entry_set_ttl(ent, -1);
    const size_t k_large_container_size = 1;
    bool too_big = false;
    switch(ent->type){
        case T_ZSET:
            too_big = hm_size(&ent->zset->hmap)> k_large_container_size;
            //cout <<"entry_del: too_big? "<<too_big<<endl;
            break;
    }

    if(too_big){
        producer(&d_data.tp, &entry_del_async, ent);
    }else{
        entry_destroy(ent);
    }
  
}
