#include "header.h"


static ZNode *znode_new(const char *name, size_t len, double score){
    //cout <<"znode_new: in"<<endl;

    //allocate size for the new struct
    ZNode *node = (ZNode *)malloc(sizeof(ZNode)+len);

    //initialise fields of tree
    avl_init(&node->tree);

    //initialise fields of hmap
    node->hmap.next = NULL;
    node->hmap.hcode = str_hash((uint8_t*)name, len);
    
    //initialise other fields
    node->score = score;
    node->len = len;
    memcpy(&node->name[0], name, len);
    return node;
}

bool z_less(AVLNode *lhs, double score, const char *name, size_t len){
    ZNode *zl = container_of(lhs, ZNode, tree);
    bool ans;
    if(zl->score != score){
        ans =zl->score < score;
        return ans;
    }
    int rv = memcmp(zl->name, name, min(zl->len, len));
    if(rv!=0){
        ans = rv<0;
        return ans;
    }
    ans = zl->len <len;
    //cout <<zl->len<<" "<<len<<endl;
    return ans;
}

bool zless(AVLNode *lhs, AVLNode *rhs){
    ZNode *zn = container_of(rhs, struct ZNode, tree);
    return z_less(lhs, zn->score, zn->name, zn->len);
}

//inset node->tree into zset->tree(which has node of a tree)
static void tree_add(ZSet *zset, ZNode *node){
    //cout <<"tree_add: in "<<endl;

    if(!zset->tree){
        zset->tree = &node->tree;
        return;
    }

    //find empty leaves in zset->tree to insert node->tree
    AVLNode *current = zset->tree;
    while(true){
        //check if node->tree is less than current
        //if less from is current->left else current->right
        AVLNode **from = zless(&node->tree, current)? &current->left : &current->right;
        //cout <<"tree_add: after zless :"<<*from<<endl;

        //if from is empty add the node->tree; update its parent
        if(!*from){
            *from = &node->tree;
            node->tree.parent = current;
            //cout <<"tree_add: gonna avl_fix\n";
            zset->tree = avl_fix(&node->tree);
            break;
        }
        //from is not empty
        current = *from;
    }

}

//update a zset with node with score
static void zset_update(ZSet *zset, ZNode *node, double score){
    //cout <<"zset_update: in "<<endl;

    //if node->score equals score return
    if(node->score == score){
        return;
    }
    //delete the avl_node
    zset->tree = avl_del(&node->tree);
    //update ints score
    node->score = score;
    //avl_node initialisaiton
    avl_init(&node->tree);
    //add node tree to the tree of zset
    tree_add(zset, node);
}

//compare two znode
//which hcode, name and len
static bool hcmp(HNode * node , HNode * key){
    if(node->hcode != key->hcode){
        return false;
    }
    ZNode *znode = container_of(node, ZNode, hmap);
    HKey *hkey = container_of(key, HKey, node);
    if(znode->len != hkey->len){
        return false;
    }
    return 0 == memcmp(znode->name, hkey->name, znode->len);
}

//return znode
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len) {
    //cout <<"zset_lookup: in\n";

    if (!zset->tree) {
        //cout <<"zset_lookup: tree of zset is null\n";
        return NULL;
    }
    HKey key;
    key.node.hcode = str_hash((uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hm_lookup(&zset->hmap, &key.node, &hcmp);
    //cout << "zset_lookup: return of hm_lookup is "<<found<<endl;
    if (!found) {
        return NULL;
    }
    return container_of(found, ZNode, hmap);
}

bool zset_add(ZSet *zset, const char *name, size_t len, double score){
//cout <<"zset_add: in "<<endl;

//lookup zset with the name   
    ZNode *node = zset_lookup(zset, name, len);

//if found update zset
//not found: create a znode. add to hm and tree
    if(node){
        zset_update(zset, node, score);
        return false;
    }
    else{
        node = znode_new(name, len, score);
        hm_insert(&zset->hmap, &node->hmap);
        tree_add(zset, node);
        return true;
    }
}

AVLNode *avl_offset(AVLNode *node, int64_t offset){
    //cout<<"avl_offset: in"<<endl;
    int64_t p = 0;
    while (offset != p)
    {
        if(p <offset && p +avl_cnt(node->right) >= offset){
            node = node->right;
            p += avl_cnt(node->left)+1;
        }else if(p > offset && p-avl_cnt(node->left) <= offset){
            node = node->left;
            p -= avl_cnt(node->right)+1;
        }else{
            AVLNode *parent = node->parent;
            if(!parent){
                //cout <<"avl_offset: return null";
                return NULL;
            }
            if(parent->right == node){
               // cout << "node is the right child of parent"<<endl;
                p -= avl_cnt(node->left)+1;
            }else{
                //cout << "node is the left child of parent"<<endl;
                p += avl_cnt(node->right)+1;
            }
            node = parent;
            //cout << "avl_offset: node is updated to at end"<<node<<endl;
        }
    }
    //cout <<"avl_offset:return node "<<node<<endl;
    return node;
}

ZNode *zset_query(ZSet *zset, double score,const char *name, size_t len, int64_t offset){
    AVLNode *found = NULL;
    AVLNode *current = zset->tree;
    while(current){
        if(z_less(current, score, name, len)){
            //cout <<" this zset is less than ours\n";
            current = current->right;
        }else{
            //cout <<" this zset is greater than ours\n";
            found = current;
            current = current->left;
        }
    }
    if(found){
        found = avl_offset(found, offset);
    }
    //cout <<"zset_query: found after offset is "<<found<<endl;
    return found? container_of(found, ZNode, tree): NULL;
}

void znode_del(ZNode *node) {
    free(node);
}

ZNode *zset_pop(ZSet *zset, const char *name, size_t len) {
    //cout <<"zset_pop: \n";

    if (!zset->tree) {
        return NULL;
    }

    HKey key;
    key.node.hcode = str_hash((uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hm_pop(&zset->hmap, &key.node, &hcmp);
    //cout <<"zset_pop: return HNode from hm_pop is "<<found<<endl;
    if (!found) {
        return NULL;
    }

    ZNode *node = container_of(found, ZNode, hmap);

    //cout <<"zset_pop: corres ZNode neame is  "<<node->name<<endl;
  
    zset->tree = avl_del(&node->tree);

    return node;
}

static void tree_dispose(AVLNode *node) {
    if (!node) {
        return;
    }
    tree_dispose(node->left);
    tree_dispose(node->right);
    znode_del(container_of(node, ZNode, tree));
}

//assumes s is double 
 bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return (endp == s.c_str() + s.size()) && (!isnan(out));
}

bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

