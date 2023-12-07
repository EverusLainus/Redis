
#include "header.h"
/*
//if we have a pointer to a member of a struct
//this is used to find the pointer to the begin of the stuct 
//which contains the member
#define container_of(ptr, type, member) ({                  \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
    (type *)( (char *)__mptr - offsetof(type, member) );})
*/
enum{
    RES_OK = 0, RES_ERR = 1, RES_NX = 2
};

const size_t resize_value = 128;
const size_t max_load_factor = 2;

//initialise a htable of size n
//set mask to n-1
static void h_init(Htable *htable, size_t n){
   // cout <<"h_init: hashtable_ptr "<<htable<< "with size "<<n<<endl;
    //chech if n is a two power
    if((n>0 && (n & (n-1)) != 0)){
       // cout << "n is not an even number\n";
        return;
    }
    //create space of array of pointer to HNode with n elts
    htable->h_arr = (HNode **) calloc(sizeof(HNode *),n);
    htable->mask = n-1;
    htable->size = 0;
}

/*
let p be the and of htable mask and hcode of node
next be a pointer to Htable with value htable h_arr p
let node's next be next
pth idx if hash array be node
*/
static void h_insert(Htable *htable, HNode *node){
    //printf("h_insert: in\n");
    size_t p = (node->hcode & htable->mask);
   
    HNode *next = htable->h_arr[p];;
  
    node->next = next;
   
    htable->h_arr[p]=node;
  
    htable->size++;
    //printf("h_insert: out\n");
}

/*
find where is a node in the given hash table
*/
static HNode ** h_lookup(Htable *htable, HNode *key, bool (*cmp)(HNode *, HNode *)){
    //cout <<"h_lookup:size "<<htable->size<<endl;
    if(htable->h_arr == NULL){
        //cout <<"h_lookup: hashtables array is null\n";
        return NULL;
    }
    size_t p = (htable->mask & key->hcode);
   // cout <<"p is "<<p<<endl;
    HNode ** from = &htable->h_arr[p];
   // cout <<"*from is "<<*from<<endl;
    while(*from != NULL){
        if(cmp(*from, key) == 1){
            return from;
        }
        from = &(*from)->next;
    }
    return NULL;
}

static HNode *h_detach(Htable *htable, HNode **from){
//printf("h_detach: in\n");
    HNode *node = *from;
//  cout << "node is "<<node<<endl;
    *from = (*from)->next;
// cout <<"contents of from is "<<(*from)<<endl;
    htable->size--;
//  cout <<"updated hash table size is "<<htable->size<<endl;
    return node;
}

static void hm_help_resize(HMap *hmap){
    //printf("hm_help_resize:\n");

    //if second entry of hash map is null return
    if(hmap->ht2.h_arr == NULL){
        //printf("hm_help_resize: 2nd hashtable is null, so we return \n");
        return;
    }

    //start counting from zero
    size_t count = 0;

    //while resize_value is less that count and 
    //size of second entry in hash map is >0
    while(count < resize_value && hmap->ht2.size >0){
        //get the resizing_from entry of 
        HNode **from = &hmap->ht2.h_arr[hmap->resizing_from];
        
        if(*from == NULL){
            hmap->resizing_from++;
            continue;
        }
        //printf("hm_help_resize: detach from ht2 to insert in ht1\n");
        HNode * detach_res = h_detach(&hmap->ht2, from);
        h_insert(&hmap->ht1, detach_res);
        count++;
    }

    if(hmap->ht2.size == 0){
        //cout <<"hash table 2 has size 0 \n";
        free(hmap->ht2.h_arr);
        hmap->ht2= Htable();
    }
}

HNode *hm_lookup(HMap *hmap, HNode *key, bool (*cmp) (HNode *, HNode *)){
   //cout <<"hm_lookup: in\n";
    hm_help_resize(hmap);

    HNode **from = h_lookup(&hmap->ht1, key, cmp);
   //printf("hm_lookup: res from h_lookup with ht1: %d\n", from);
    if(from == NULL){
        from = h_lookup(&hmap->ht2, key, cmp);
       // printf("hm_lookup: res from h_lookup with ht2: %d\n", from);
    }
    return (from? *from : NULL);
}

static void start_resizing(HMap *hmap){
    //check if second hash table of hash map is null
    //cout <<"start_resizing\n";
    assert(hmap->ht2.h_arr == NULL); 
    hmap->ht2 = hmap->ht1;
/*
    for(int i =0; i<4; i++){
        cout << hmap->ht2.h_arr[i]<<" ";
    } 
    cout <<endl;
*/
    h_init(&hmap->ht1, (hmap->ht1.mask +1) * 2);
    hmap->resizing_from = 0;
}

void hm_insert (HMap *hmap, HNode *node){
    //printf("hm insert: in\n");

    //if first hash table is null give is some memory
    if(hmap->ht1.h_arr == NULL){
       // cout <<"first hash table is null\n";
        h_init( &hmap->ht1, 4);
    }

    //insert a node in first hash table
    h_insert(&hmap->ht1, node);
    //printf("hm_insert: outa h_insert for ht1\n");

    //chech if second hash table 
    if(!hmap->ht2.h_arr){
        size_t load_factor = hmap->ht1.size/(hmap->ht1.mask +1);
       // cout <<"mask +1 is "<<hmap->ht1.mask +1<<endl;
       // cout <<"load factor is "<<load_factor<<endl;
        if(load_factor >= max_load_factor){
            //printf("hm_insert: gonna start_resizing\n");
            start_resizing(hmap);
        }
    }
    hm_help_resize(hmap);
}

HNode * hm_pop(HMap *hmap, HNode *key, bool (*cmp)(HNode *, HNode *)){
   hm_help_resize(hmap);

   HNode **from = h_lookup(&hmap->ht1, key, cmp);
   if(from){
    HNode *h_detach_res = h_detach(&hmap->ht1, from);
    return h_detach_res;
   } 
   from = h_lookup(&hmap->ht2, key, cmp);
   if(from){
    return h_detach(&hmap->ht1, from);
   }
   return NULL;
}


void hm_destroy(HMap *hmap) {
    //cout <<"hm_destroy\n";
    free(hmap->ht1.h_arr);
    free(hmap->ht2.h_arr);
    *hmap = HMap();
}

int str_hash(uint8_t * x, int size){
   // cout <<"str_hash:"<<x<<endl;
    uint32_t h = 0x811c9dc5;
    for(size_t i = 0; i<size; i++){
        h = (h+x[i] * 0x01000193);
    }
   // cout <<"hash is "<<h<<endl;
    return h;
}

bool entry_eq(HNode *lhs, HNode *rhs){
    struct Entry *left = container_of(lhs, struct Entry, node);
    struct Entry *right = container_of (rhs, struct Entry, node);
    return lhs->hcode == rhs->hcode && left->key == right->key;
}

