
/**********************************************************************************************/

/*
  Order matching engine
  Eugene Logunov `2011
  
  Concept:
    Orderbook is implemented as a pair of two identical structures (t_orderbook_side).
    Each structure corresponds an order side (so, asks and bids are stored separately).
    
    Each orderbook side contains a hashtable and a red-black dynamic tree.
    
    Hashtable is static (never growing; its size is HASHTAB_SIZE); collisions are resolved
    via chaining. Furthermore, hashtable allows iteration through its nodes with respect
    to priority (which is price-time) using flink/blink_priority pointers of t_hashtab_node.
    
    When placing limit order - it is inserted into hashtable using order id as a key. So,
    it is possible to find order in O(loadFactor) time (required for cancel(); loadFactor
    in the worst case would be (MAX_LIVE_ORDERS / HASHTAB_SIZE), assuming that hash function
    works fine). Insertion into hashtable is O(1).
    
    As for iteration of hashtable nodes - it performs usage of red-black tree. When order
    is inserted into hashtable - the corresponding node should have flink/blink_pointers
    initialized (the node should be inserted before/after the latest node, which has the
    same price). Red-black tree performs mapping from price to the node, corresponding the
    latest order placed at that price. Operations with tree are log(numPriceLevels), where
    numPriceLevels in the worst case would be (MAX_PRICE - MIN_PRICE). Finding next node
    with respect to priority takes O(1) time. As a result, orders matching is very fast.
    
    Memory for hashtable/tree nodes is allocated from static stack-based pools. Each pool
    is a block of memory (large enough for worst cases - max number of orders or max number
    of price levels) and a stack. Block is an array of structures with necessary type. When
    pool is initialized - stack is filled with pointers to elements of block. When allocating
    node - a pointer is popped from stack and returned. When deallocating node - a pointer is
    pushed to stack. This approach allows to allocate/free memory very fast (to compare with
    malloc()/free()); furthermore, memory deallocation on deinitialization is not required.
*/

/**********************************************************************************************/

#include "engine.h"

#ifndef NULL
#define NULL 0
#endif

#define FALSE 0
#define TRUE 1

/**********************************************************************************************/

// Settings for hashtables.

/*
  Number of significant bits in hash.
*/
#define HASH_BITS 10

/*
  This implementation uses 32-bit hash function, so
  masking is required to extract HASH_BITS bits.
*/
#define HASH_MASK (0xFFFFFFFF >> (32 - HASH_BITS))

/*
  Size of hashtables.
  
  Hashtables are static (never grow); collisions are
  resolved via chaining.
*/
#define HASHTAB_SIZE (1 << HASH_BITS)

/*
  Max number of hashtable nodes.
  
  There are two hashtables, each requires (HASHTAB_SIZE + 1)
  nodes for sentinels; furthermore, one node is required
  per order placed.
  
  Nodes are allocated from a static pool.
*/
#define HASHTAB_MAX_NODES (MAX_LIVE_ORDERS + 2 * (HASHTAB_SIZE + 1))

/**********************************************************************************************/

// Settings for red-black trees.

/*
  Possible colors of tree nodes.
*/
#define COLOR_BLACK 0
#define COLOR_RED 1

/*
  Max number of tree nodes.
  
  There are two trees, each requires a single node
  for sentinel; furthermore, one node is required
  per price level (number of active price levels
  couldn't exceed the max number of live orders).
  
  Nodes are allocated from a static pool.
*/
#define TREE_MAX_NODES (MAX_LIVE_ORDERS + 2)

/*
  Price for tree sentinel nodes.
*/
#define TREE_SENTINEL_PRICE (MAX_PRICE + 1)

/**********************************************************************************************/

// Internal types.

/*
  Hash function value (masked by HASH_MASK).
*/
typedef int t_hash;

/*
  Hash table node structure.
  
  Nodes store orderids and corresponding orders.
  Nodes with the same hash are doubly-linked using
  flink/blink_chain; furthermore, non-sentinel nodes
  (except two sentinels not stored into hashtable)
  are doubly-linked according to price-time priority
  using flink/blink_priority.
  
  There is also a tree_link, which contains non-NULL
  value if the order has the lowest priority for its
  price (so a new order with the same price will be
  inserted immediately before/after this node in
  flink/blink_priority double-linked list). Pointer
  may be NULL in case when there are another orders
  with the same price, but lower priority (when such
  node is removed from hashtable - tree modification
  is not required).
*/
typedef struct t_hashtab_node_internal {
  struct t_hashtab_node_internal* flink_chain;
  struct t_hashtab_node_internal* blink_chain;
  t_orderid orderid;
  struct t_hashtab_node_internal* flink_priority;
  struct t_hashtab_node_internal* blink_priority;
  t_order order;
  struct t_tree_node_internal* tree_link;
} t_hashtab_node;

/*
  Extended price for tree nodes.
  
  Required for the case when TREE_SENTINEL_PRICE
  can't be stored correctly into t_price.
*/
typedef int t_tree_price;

/*
  Red-black tree node structure.
  
  Node stores a link to hashtable node, after/before
  which a new node with given price should be inserted
  into flink/blink_priority double-linked list. Pointer
  may be NULL for just-created node; in that case pointer
  is initialized using previous/next tree node.
*/
typedef struct t_tree_node_internal {
  struct t_tree_node_internal* parent;
  struct t_tree_node_internal* left;
  struct t_tree_node_internal* right;
  t_tree_price price;
  int color;
  t_hashtab_node* hashtab_link;
} t_tree_node;

/*
  Orderbook side structure.
  
  Each side contains:
  hashtab_chains - a static hashtable (an array of
    pointers to double-linked lists sentinels);
  hashtab_sentinel - sentinel node, which is not
    stored into hashtable, but linked into
    flink/blink_priority double-linked list;
  tree_root - red-black tree root node (initially,
    a sentinel node);
  tree_sentinel - a sentinel node for red-black tree.
  
  hashtab_sentinel and tree_sentinel are linked using
  tree_link and hashtab_link (simplifies the insertion
  into flink/blink_priority double-linked list).
  
  Engine uses two instances of this structure, one for
  bids and another for asks.
*/
typedef struct {
  t_hashtab_node* hashtab_chains[HASHTAB_SIZE];
  t_hashtab_node* hashtab_sentinel;
  t_tree_node* tree_root;
  t_tree_node* tree_sentinel;
} t_orderbook_side;

/**********************************************************************************************/

// Order id generator.

/*
  Next order id to be returned.
*/
t_orderid next_orderid;

/*
  Initialization of order id generator.
*/
inline void orderid_init() {
  next_orderid = 1;
}

/*
  Generation of order id.
*/
inline t_orderid orderid_generate() {
  return next_orderid++;
}

/**********************************************************************************************/

// Hashtable nodes pool.

/*
  Hashtable nodes pool memory block. Just an array
  of nodes with required capacity.
*/
t_hashtab_node hashtab_pool_memblock[HASHTAB_MAX_NODES];

/*
  Hashtable nodes pool stack.
  
  Initially, contains pointers to each element of
  memblock; on allocation a pointer is popped; on
  deallocation a pointer is pushed.
  
  As stack grows, the address of its top decreases.
*/
t_hashtab_node* hashtab_pool_stack[HASHTAB_MAX_NODES];

/*
  Counter of allocated nodes. Also serves as a
  stack top index.
*/
int hashtab_pool_counter;

/*
  Initialization of hashtable nodes pool.
*/
inline void hashtab_pool_init() {
  hashtab_pool_counter = 0;

  int i;
  for (i = 0; i < HASHTAB_MAX_NODES; i++)
    hashtab_pool_stack[i] = &hashtab_pool_memblock[i];
}

/*
  Allocation of hashtable node.
*/
inline t_hashtab_node* hashtab_pool_alloc() {
  return hashtab_pool_stack[hashtab_pool_counter++];
}

/*
  Deallocation of hashtable node.
  Not required for sentinel nodes.
*/
inline void hashtab_pool_dealloc(t_hashtab_node* node) {
  hashtab_pool_stack[--hashtab_pool_counter] = node;
}

/**********************************************************************************************/

// Red-black tree nodes pool.

/*
  Tree nodes pool memory block. Just an array
  of nodes with required capacity.
*/
t_tree_node tree_pool_memblock[TREE_MAX_NODES];

/*
  Tree nodes pool stack.
  
  Initially, contains pointers to each element of
  memblock; on allocation a pointer is popped; on
  deallocation a pointer is pushed.
  
  As stack grows, the address of its top decreases.
*/
t_tree_node* tree_pool_stack[TREE_MAX_NODES];

/*
  Counter of allocated nodes. Also serves as a
  stack top index.
*/
int tree_pool_counter;

/*
  Initialization of tree nodes pool.
*/
inline void tree_pool_init() {
  tree_pool_counter = 0;

  int i;
  for (i = 0; i < TREE_MAX_NODES; i++)
    tree_pool_stack[i] = &tree_pool_memblock[i];
}

/*
  Allocation of tree node.
*/
inline t_tree_node* tree_pool_alloc() {
  return tree_pool_stack[tree_pool_counter++];
}

/*
  Deallocation of tree node.
  Not required for sentinel nodes.
*/
inline void tree_pool_dealloc(t_tree_node* node) {
  tree_pool_stack[--tree_pool_counter] = node;
}

/**********************************************************************************************/

// Hashtable.

/*
  Performs mapping: t_orderid -> t_hash.
*/
/*
inline t_hash hashfunc_calc(t_orderid orderid) {
  t_hash hash = orderid;
  hash += ~(hash << 16);
  hash ^= (hash >> 5);
  hash += (hash << 3);
  hash ^= (hash >> 13);
  hash += ~(hash << 9);
  hash ^= (hash >> 17);
  return hash & HASH_MASK;
}
*/
inline t_hash hashfunc_calc(t_orderid orderid) {
  t_hash hash = orderid ^ (orderid >> 16);
  return hash & HASH_MASK;
}

/*
  Allocates sentinel nodes and performs
  initial linking.
*/
inline void hashtab_init(t_orderbook_side* ob_side) {
  t_hashtab_node* node = hashtab_pool_alloc();
  node -> flink_priority = node;
  node -> blink_priority = node;
  ob_side -> hashtab_sentinel = node;

  int i;
  for (i = 0; i < HASHTAB_SIZE; i++) {
    t_hashtab_node* node = hashtab_pool_alloc();
    node -> flink_chain = node;
    node -> blink_chain = node;
    ob_side -> hashtab_chains[i] = node;
  }
}

/*
  Returns a pointer to hashtable node; the node
  is not inserted into flink/blink_priority
  double-linked list.
*/
inline t_hashtab_node* hashtab_insert(t_orderbook_side* ob_side, t_order* order) {
  t_hashtab_node* node = hashtab_pool_alloc();

  t_orderid orderid = orderid_generate();
  t_hash hash = hashfunc_calc(orderid);
  t_hashtab_node* chain_start = ob_side -> hashtab_chains[hash];
  t_hashtab_node* chain_end = chain_start -> blink_chain;
  chain_end -> flink_chain = node;
  chain_start -> blink_chain = node;

  node -> flink_chain = chain_start;
  node -> blink_chain = chain_end;
  node -> orderid = orderid;
  node -> order = *order;

  return node;
}

/*
  Returns NULL if not found.
*/
inline t_hashtab_node* hashtab_find(t_orderbook_side* ob_side, t_orderid orderid) {
  t_hash hash = hashfunc_calc(orderid);
  t_hashtab_node* chain_start = ob_side -> hashtab_chains[hash];

  t_hashtab_node* node = chain_start -> flink_chain;
  while (node != chain_start) {
    if (node -> orderid == orderid)
      return node;
    node =  node -> flink_chain;
  }

  return NULL;
}

/*
  Node must be already removed from
  flink/blink_priority double-linked list.
*/
inline void hashtab_remove(t_hashtab_node* node) {
  t_hashtab_node* next = node -> flink_chain;
  t_hashtab_node* prev = node -> blink_chain;

  prev -> flink_chain = next;
  next -> blink_chain = prev;

  hashtab_pool_dealloc(node);
}

/**********************************************************************************************/

// Red-black tree.

/*
  Allocates sentinel nodes and performs
  initial linking.
*/
inline void tree_init(t_orderbook_side* ob_side) {
  t_tree_node* node = tree_pool_alloc();

  node -> parent = NULL;
  node -> left = node;
  node -> right = node;
  node -> price = TREE_SENTINEL_PRICE;
  node -> color = COLOR_BLACK;

  ob_side -> tree_root = node;
  ob_side -> tree_sentinel = node;
}

/*
  Returns tree_sentinel if not found.
*/
inline t_tree_node* tree_find_next(t_orderbook_side* ob_side, t_tree_node* node) {
  if (node -> right != ob_side -> tree_sentinel) {
    node = node -> right;
    while (node -> left != ob_side -> tree_sentinel)
      node = node -> left;
    return node;
  }

  t_tree_node* tmp = node -> parent;
  while (tmp && (tmp != ob_side -> tree_sentinel) && (node == tmp -> right)) {
    node = tmp;
    tmp = tmp -> parent;
  }

  if (tmp)
    return tmp;
  else
    return ob_side -> tree_sentinel;
}

/*
  Returns tree_sentinel if not found.
*/
inline t_tree_node* tree_find_prev(t_orderbook_side* ob_side, t_tree_node* node) {
  if (node -> left != ob_side -> tree_sentinel) {
    node = node -> left;
    while (node -> right != ob_side -> tree_sentinel)
      node = node -> right;
    return node;
  }

  t_tree_node* tmp = node -> parent;
  while (tmp && (tmp != ob_side -> tree_sentinel) && (node == tmp -> left)) {
    node = tmp;
    tmp = tmp -> parent;
  }

  if (tmp)
    return tmp;
  else
    return ob_side -> tree_sentinel;
}

/*
  Tree left rotation.
  An internal function for tree balancing.
*/
inline void tree_rotate_left(t_orderbook_side* ob_side, t_tree_node* node) {
  t_tree_node* tmp = node -> right;

  node -> right = tmp -> left;
  if (tmp -> left != ob_side -> tree_sentinel)
    tmp -> left -> parent = node;
  if (tmp != ob_side -> tree_sentinel)
    tmp -> parent = node -> parent;

  if (node -> parent) {
    if (node == node -> parent -> left)
      node -> parent -> left = tmp;
    else
      node -> parent -> right = tmp;
  }
  else
    ob_side -> tree_root = tmp;

  tmp -> left = node;
  if (node != ob_side -> tree_sentinel)
    node -> parent = tmp;
}

/*
  Tree right rotation.
  An internal function for tree balancing.
*/
inline void tree_rotate_right(t_orderbook_side* ob_side, t_tree_node* node) {
  t_tree_node* tmp = node -> left;

  node -> left = tmp -> right;
  if (tmp -> right != ob_side -> tree_sentinel)
    tmp -> right -> parent = node;
  if (tmp != ob_side -> tree_sentinel)
    tmp -> parent = node -> parent;

  if (node -> parent) {
    if (node == node -> parent -> right)
      node -> parent -> right = tmp;
    else
      node -> parent -> left = tmp;
  }
  else
    ob_side -> tree_root = tmp;

  tmp -> right = node;
  if (node != ob_side -> tree_sentinel)
    node -> parent = tmp;
}

/*
  Tree fixup/rebalancing on node insertion.
  Resolves violations of red-black tree properties.
*/
inline void tree_fixup_on_insert(t_orderbook_side* ob_side, t_tree_node* node) {
  while ((node != ob_side -> tree_root) && (node -> parent -> color == COLOR_RED)) {
    if (node -> parent == node -> parent -> parent -> left) {
      t_tree_node* tmp = node -> parent -> parent -> right;
      if (tmp -> color == COLOR_RED) {
        node -> parent -> color = COLOR_BLACK;
        tmp -> color = COLOR_BLACK;
        node -> parent -> parent -> color = COLOR_RED;
        node = node -> parent -> parent;
      }
      else {
        if (node == node -> parent -> right) {
          node = node -> parent;
          tree_rotate_left(ob_side, node);
        }
        node -> parent -> color = COLOR_BLACK;
        node -> parent -> parent -> color = COLOR_RED;
        tree_rotate_right(ob_side, node -> parent -> parent);
      }
    }
    else {
      t_tree_node* tmp = node -> parent -> parent -> left;
      if (tmp -> color == COLOR_RED) {
        node -> parent -> color = COLOR_BLACK;
        tmp -> color = COLOR_BLACK;
        node -> parent -> parent -> color = COLOR_RED;
        node = node -> parent -> parent;
      }
      else {
        if (node == node -> parent -> left) {
          node = node -> parent;
          tree_rotate_right(ob_side, node);
        }
        node -> parent -> color = COLOR_BLACK;
        node -> parent -> parent -> color = COLOR_RED;
        tree_rotate_left(ob_side, node -> parent -> parent);
      }
    }
  }
  ob_side -> tree_root -> color = COLOR_BLACK;
}

/*
  Tree fixup/rebalancing on node removal.
  Resolves violations of red-black tree properties.
*/
inline void tree_fixup_on_remove(t_orderbook_side* ob_side, t_tree_node* node) {
  while ((node != ob_side -> tree_root) && (node -> color == COLOR_BLACK)) {
    if (node == node -> parent -> left) {
      t_tree_node* tmp = node -> parent -> right;
      if (tmp -> color == COLOR_RED) {
        tmp -> color = COLOR_BLACK;
        node -> parent -> color = COLOR_RED;
        tree_rotate_left(ob_side, node -> parent);
        tmp = node -> parent -> right;
      }
      if ((tmp -> left -> color == COLOR_BLACK) && (tmp -> right -> color == COLOR_BLACK)) {
        tmp -> color = COLOR_RED;
        node = node -> parent;
      }
      else {
        if (tmp -> right -> color == COLOR_BLACK) {
          tmp -> left -> color = COLOR_BLACK;
          tmp -> color = COLOR_RED;
          tree_rotate_right(ob_side, tmp);
          tmp = node -> parent -> right;
        } 
        tmp -> color = node -> parent -> color;
        node -> parent -> color = COLOR_BLACK;
        tmp -> right -> color = COLOR_BLACK;
        tree_rotate_left(ob_side, node -> parent);
        node = ob_side -> tree_root;
      }
    }
    else {
      t_tree_node* tmp = node -> parent -> left;
      if (tmp -> color == COLOR_RED) {
        tmp -> color = COLOR_BLACK;
        node -> parent -> color = COLOR_RED;
        tree_rotate_right(ob_side, node -> parent);
        tmp = node -> parent -> left;
      }
      if ((tmp -> right -> color == COLOR_BLACK) && (tmp -> left -> color == COLOR_BLACK)) {
        tmp -> color = COLOR_RED;
        node = node -> parent;
      }
      else {
        if (tmp -> left -> color == COLOR_BLACK) {
          tmp -> right -> color = COLOR_BLACK;
          tmp -> color = COLOR_RED;
          tree_rotate_left(ob_side, tmp);
          tmp = node -> parent -> left;
        }
        tmp -> color = node -> parent -> color;
        node -> parent -> color = COLOR_BLACK;
        tmp -> left -> color = COLOR_BLACK;
        tree_rotate_right(ob_side, node -> parent);
        node = ob_side -> tree_root;
      }
    }
  }
  node -> color = COLOR_BLACK;
}

/*
  Returns pointer to new node. Existing node
  is returned if possible. If node was created -
  hashtab_link must be initialized.
*/
inline t_tree_node* tree_insert(t_orderbook_side* ob_side, t_price price) {
  t_tree_node* current = ob_side -> tree_root;
  t_tree_node* parent = NULL;
  while (current != ob_side -> tree_sentinel) {
    if ((t_tree_price)price == current -> price)
      return current;
    parent = current;
    if ((t_tree_price)price < current -> price)
      current = current -> left;
    else
      current = current -> right;
  }

  t_tree_node* node = tree_pool_alloc();
  node -> parent = parent;
  node -> left = ob_side -> tree_sentinel;
  node -> right = ob_side -> tree_sentinel;
  node -> price = (t_tree_price)price;
  node -> color = COLOR_RED;
  node -> hashtab_link = NULL;

  if (parent) {
    if (price < parent -> price)
      parent -> left = node;
    else
      parent -> right = node;
  }
  else
    ob_side -> tree_root = node;

  tree_fixup_on_insert(ob_side, node);

  return node;
}

/*
  Node must not be referenced hashtable nodes.
*/
inline void tree_remove(t_orderbook_side* ob_side, t_tree_node* node) {
  if (!node || (node == ob_side -> tree_sentinel))
    return;

  t_tree_node* tmp_1;
  if ((node -> left == ob_side -> tree_sentinel) || (node -> right == ob_side -> tree_sentinel))
    tmp_1 = node;
  else {
    tmp_1 = node -> right;
    while (tmp_1 -> left != ob_side -> tree_sentinel)
      tmp_1 = tmp_1 -> left;
  }

  t_tree_node* tmp_2;
  if (tmp_1 -> left != ob_side -> tree_sentinel)
    tmp_2 = tmp_1 -> left;
  else
    tmp_2 = tmp_1 -> right;

  tmp_2 -> parent = tmp_1 -> parent;
  if (tmp_1 -> parent) {
    if (tmp_1 == tmp_1 -> parent -> left)
      tmp_1 -> parent -> left = tmp_2;
    else
      tmp_1 -> parent -> right = tmp_2;
  }
  else
    ob_side -> tree_root = tmp_2;

  if (tmp_1 != node) {
    node -> price = tmp_1 -> price;
    node -> hashtab_link = tmp_1 -> hashtab_link;
  }

  if (tmp_1 -> color == COLOR_BLACK)
    tree_fixup_on_remove(ob_side, tmp_2);

  tree_pool_dealloc(tmp_1);
}

/**********************************************************************************************/

// Order matching engine core.

/*
  Initializes hashtable, tree; links sentinels.
*/
inline void mte_init(t_orderbook_side* ob_side) {
  hashtab_init(ob_side);
  tree_init(ob_side);

  ob_side -> tree_sentinel -> hashtab_link = ob_side -> hashtab_sentinel;
  ob_side -> hashtab_sentinel -> tree_link = ob_side -> tree_sentinel;
}

/*
  Removes node from flink/blink_priority
  double-linked list; maintains tree nodes
  pointers to hashtable nodes; removes tree
  nodes if necessary; finally, removes node
  from hashtable.
*/
inline void mte_remove_order(t_orderbook_side* ob_side, t_hashtab_node* node) {
  t_hashtab_node* prev_hashtab_node = node -> blink_priority;
  t_hashtab_node* next_hashtab_node = node -> flink_priority;
  t_tree_node* tree_node = node -> tree_link;

  if (tree_node) {
    if (!is_ask((node -> order).side)) {
      if ((next_hashtab_node -> order).price == (node -> order).price) {
        next_hashtab_node -> tree_link = tree_node;
        tree_node -> hashtab_link = next_hashtab_node;
      }
      else
        tree_remove(ob_side, tree_node);
    }
    else {
      if ((prev_hashtab_node -> order).price == (node -> order).price) {
        prev_hashtab_node -> tree_link = tree_node;
        tree_node -> hashtab_link = prev_hashtab_node;
      }
      else
        tree_remove(ob_side, tree_node);
    }
  }

  prev_hashtab_node -> flink_priority = next_hashtab_node;
  next_hashtab_node -> blink_priority = prev_hashtab_node;

  hashtab_remove(node);
}

/*
  Sends execution reports; updates sizes
  of orders after execution.
*/
inline void mte_cross_orders(t_order* old_order, t_order* new_order) {
  t_size trade_size = old_order -> size;
  if (trade_size > new_order -> size)
    trade_size = new_order -> size;

  t_price new_order_saved_price = new_order -> price;
  t_size new_order_saved_size = new_order -> size - trade_size;
  t_size old_order_saved_size = old_order -> size - trade_size;

  new_order -> price = old_order -> price;
  new_order -> size = trade_size;
  old_order -> size = trade_size;

  execution((t_execution)*new_order);
  execution((t_execution)*old_order);

  new_order -> price = new_order_saved_price;
  new_order -> size = new_order_saved_size;
  old_order -> size = old_order_saved_size;
}

/*
  Iterates orderbook side and crosses orders.
  
  Returns TRUE if order was filled (so it
  is not placed to orderbook).
*/
inline int mte_match_order(t_orderbook_side* ob_side, t_order* order) {
  t_hashtab_node* first_node = ob_side -> hashtab_sentinel;

  if (!is_ask(order -> side)) {
    t_hashtab_node* cur_node = first_node -> flink_priority;
    while ((cur_node != first_node) && ((cur_node -> order).price <= order -> price)) {
      mte_cross_orders(&(cur_node -> order), order);

      t_hashtab_node* next_node = cur_node -> flink_priority;
      if ((cur_node -> order).size == 0)
        mte_remove_order(ob_side, cur_node);
      cur_node = next_node;

      if (order -> size == 0)
        return TRUE;
    }
  }
  else {
    t_hashtab_node* cur_node = first_node -> blink_priority;
    while ((cur_node != first_node) && ((cur_node -> order).price >= order -> price)) {
      mte_cross_orders(&(cur_node -> order), order);

      t_hashtab_node* next_node = cur_node -> blink_priority;
      if ((cur_node -> order).size == 0)
        mte_remove_order(ob_side, cur_node);
      cur_node = next_node;

      if (order -> size == 0)
        return TRUE;
    }
  }

  return FALSE;
}

/*
  Inserts order to hashtable; inserts order
  price to tree; initializes tree node if
  required; inserts new hashtable node into
  flink/blink_priority double-linked list.
  
  Returns order id.
*/
inline t_orderid mte_place_order(t_orderbook_side* ob_side, t_order* order) {
  t_hashtab_node* hashtab_node = hashtab_insert(ob_side, order);
  t_tree_node* tree_node = tree_insert(ob_side, order -> price);

  t_hashtab_node* prev_hashtab_node;
  t_hashtab_node* next_hashtab_node;

  if (!is_ask(order -> side)) {
    if (tree_node -> hashtab_link) {
      next_hashtab_node = tree_node -> hashtab_link;
      next_hashtab_node -> tree_link = NULL;
    }
    else
      next_hashtab_node = tree_find_next(ob_side, tree_node) -> hashtab_link;
    prev_hashtab_node = next_hashtab_node -> blink_priority;
  }
  else {
    if (tree_node -> hashtab_link) {
      prev_hashtab_node = tree_node -> hashtab_link;
      prev_hashtab_node -> tree_link = NULL;
    }
    else
      prev_hashtab_node = tree_find_prev(ob_side, tree_node) -> hashtab_link;
    next_hashtab_node = prev_hashtab_node -> flink_priority;
  }

  prev_hashtab_node -> flink_priority = hashtab_node;
  next_hashtab_node -> blink_priority = hashtab_node;
  hashtab_node -> flink_priority = next_hashtab_node;
  hashtab_node -> blink_priority = prev_hashtab_node;
  hashtab_node -> tree_link = tree_node;
  tree_node -> hashtab_link = hashtab_node;

  return hashtab_node -> orderid;
}

/*
  Removes order if found.
*/
inline void mte_cancel_order(t_orderbook_side* ob_side, t_orderid orderid) {
  t_hashtab_node* node = hashtab_find(ob_side, orderid);
  if (node)
    mte_remove_order(ob_side, node);
}

/**********************************************************************************************/

// Order matching engine interface.

/*
  Instances of internal structures for
  order matching engine.
*/
t_orderbook_side bids, asks;

void init() {
  orderid_init();
  hashtab_pool_init();
  tree_pool_init();
  mte_init(&bids);
  mte_init(&asks);
}

void destroy() {
  // intentionally blank
}

t_orderid limit(t_order order) {
  if (!is_ask(order.side)) {
    if (!mte_match_order(&asks, &order))
      return mte_place_order(&bids, &order);
    return orderid_generate();
  }
  else {
    if (!mte_match_order(&bids, &order))
      return mte_place_order(&asks, &order);
    return orderid_generate();
  }
}

void cancel(t_orderid orderid) {
  mte_cancel_order(&bids, orderid);
  mte_cancel_order(&asks, orderid);
}

/**********************************************************************************************/
