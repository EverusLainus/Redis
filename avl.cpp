#include "header.h"


//initialise depth and cnt as 1 and left, right and parent as NULL
void avl_init(AVLNode *node){
    node->depth = 1;
    node->cnt =1;
    node->left = node->right = node->parent = NULL;
}

//returns the depth of the node
uint32_t avl_depth(AVLNode *node){
    return node? node->depth : 0;
}

//returns the cnt of the node
uint32_t avl_cnt(AVLNode *node){
    return node? node->cnt : 0;
}

//returns the max of two uint32_t values
uint32_t max(uint32_t lhs, uint32_t rhs){
    return lhs< rhs ?rhs : lhs;
}

//updating depth and cnt field
//depth of node is max of depth of right and left node
//cnt is 1+addition of cnt of right and left child
void avl_update(AVLNode *node){
   // cout<<"avl_update: in"<<endl;   
    node->depth = 1+ max(avl_depth(node->left), avl_depth(node->right));   
    node->cnt = 1+ avl_cnt(node->left) +avl_cnt(node->right);
}

//balancing by rotaiton
//left rotaion
AVLNode *rot_left(AVLNode *node){
    //cout<<"rot_left: in with node "<< node<<endl;
    AVLNode *new_node = node->right;
    //cout << "new_node is" <<new_node<<endl;
    if(new_node->left){
        new_node->left->parent = node;
        //cout << "new_node->left->parent " <<new_node->left->parent <<endl;
    }
    node->right = new_node->left;
    new_node->left = node;
    new_node->parent= node->parent;
    node->parent = new_node;
    avl_update(node);
    avl_update(new_node);
    //cout << "new node is "<<new_node<<endl;
    return new_node;
}

//right rotation
AVLNode *rot_right(AVLNode *node){
    //cout<<"rot_right: in with node "<< node<<endl;
    AVLNode *new_node = node->left;
    //cout << "new_node is" <<new_node<<endl;
    if(new_node->right){
        new_node->right->parent = node;
        //cout << "new_node->right->parent " <<new_node->right->parent <<endl;
    }
    node->left = new_node->right;
    //cout << "node->left" <<node->left<<endl;
    new_node->right = node;
    new_node->parent = node->parent; 
    node->parent = new_node;
    avl_update(node);
    avl_update(new_node);
    //cout << "new node is "<<new_node<<endl;
    return new_node;
}

//balance left subtree from avl_Fix
AVLNode *avl_fix_left(AVLNode *root){
    //cout<<"avl_fix_left: in"<<endl;
    if(avl_depth(root->left->left ) < avl_depth(root->left->right)){
        root->left = rot_left(root->left);
    }
    return rot_right(root);
}

//balance right subtree from avl_Fix
AVLNode *avl_fix_right(AVLNode *root){
    //cout<<"avl_fix_right: in"<<endl;
    if(avl_depth(root->right->right ) < avl_depth(root->right->left)){
        root->right = rot_right(root->right);
    }
    return rot_left(root);
}

//after adding we fix
 AVLNode *avl_fix(AVLNode *node){
   //std::cout<<"avl_fix: in with node"<< node<<endl;
    while(true){
        //cout<<"avl_fix: whie\n";
        avl_update(node);
       // cout<<"avl_fix: af avl_update"<<endl;
        uint32_t l = avl_depth(node->left);
        uint32_t r = avl_depth(node->right);
        AVLNode **from = NULL;
        if(node->parent){
            from = (node->parent->left == node)? &node->parent->left : &node->parent->right;
        }
        if(l==r+2){
            node= avl_fix_left(node);
        }else if(r==l+2){
            node= avl_fix_right(node);
        }
        //if from is null
        if(!from){
            //std::cout<<"avl_fix: from is null so reutrn node "<<node <<endl;
            return node;
        }
        //cout<<"avl_fix: from is not null"<<endl;
        *from = node;
        //cout<<"avl_fix:update from "<<from<<endl;
        node= node->parent;
        //std::cout<<"node update is "<<node<<endl;
    }
}

//delete a node
 AVLNode *avl_del(AVLNode *node){
    //cout<<"avl_del: nodeis"<<node<<endl;
    if(node->right == NULL){
        //cout<<"avl_del: right child is null\n ";
        AVLNode *parent = node->parent;
        if(node->left){
            node->left->parent = parent;
        }
        //if right cild one exist replace it with node
        if(parent){
            //cout<<"avl_del: node has a parent\n ";
            (parent->left == node? parent->left : parent->right)= node->left;
            return avl_fix(parent);

        }
        else{
            return node->left;
        }
    }else{
        //cout<<"avl_del: right child is not null\n ";
        AVLNode *victim = node->right;
        while(victim->left){
            victim = victim->left;
        }
        AVLNode *root = avl_del(victim);
        *victim = *node;
        if(victim->left){
            victim->left->parent = victim;
        }
        if(victim->right){
            victim->right->parent = victim;
        }
        AVLNode *parent = node->parent;
        if(parent){
            (parent->left == node ? parent->left : parent->right)= victim;
            return root;
        }else{
            //removing root
            return victim;
        }
    }
}

//add given container and value - used adding and testing
void add(container &c, uint32_t val){
    //cout<<"add: in"<<endl;
    Data *data = new Data();
    avl_init(&data->node);
    data->val = val;
    if(!c.root){
        c.root = &data->node;
        return;
    }

    AVLNode *current = c.root;
    //cout<<"add: current is "<<current<<endl;
    while(true){
        //cout<<"add: while\n";
        uint32_t v = container_of(current, struct Data, node)->val;
        //cout <<"value from the container "<<v<<endl;
        AVLNode **from = ((val <v )? &current->left : &current->right);
        //cout<<"from is "<< from <<" &current->left: "<< &current->left << " &current->right: "<<&current->right<<endl;
        if(!*from){
            //cout<<"add: *from is null "<<endl;
            *from = &data->node;
            data->node.parent = current;
            c.root = avl_fix(&data->node);
            break;
        }
        current = *from;
       //cout<<"add: af update  current"<<current<<endl;
    }
}

//delete given contianer and value - used by test delete
bool del(container &c, uint32_t val){
    AVLNode *current = c.root;
    //cout<<"del: current -"<<current<<endl;
    while(current){
        uint32_t node_val = container_of(current, struct Data, node)->val;
       // cout<<"del:node_val -"<<node_val<<endl;
        if(val == node_val){
            //cout<<" current's val is same as given val"<<endl;
            break;
        }
        current = val <node_val? current->left : current->right;
    }
    if(!current){
        return false;
    }
    c.root = avl_del(current);
    delete container_of(current,struct Data, node);
    return true;
}

 void avl_verify(AVLNode *parent, AVLNode *node){
    //if node is null return
    if(!node){
        return;
    }

    //check if the parent is node's parent
    assert(node->parent == parent);

    //avl verify for left and right node
    avl_verify(node, node->left);
    avl_verify(node, node->right);

    //check if cnt of the node is correct
    assert(node->cnt == 1+ avl_cnt(node->left) + avl_cnt(node->right));

    //check if the depth of node is correct
    uint32_t l = avl_depth(node->left);
    uint32_t r = avl_depth(node->right);
    assert(l==r || l+1 ==r || l== r+1);
    assert(node->depth == 1+max(l, r));

    //check if the parent of left/right child is node 
    //check val of left child is less that node's value which in turn is greater than right child's value
    uint32_t val = container_of(node, struct Data, node)->val;
    if(node->left){
        assert(node->left->parent == node);
        assert((container_of(node->left, struct Data, node)->val) <= val);
    }
    if(node->right){
        assert(node->right->parent == node);
        assert((container_of(node->right, struct Data, node)->val) >= val);
    }
}
