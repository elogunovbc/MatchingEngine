
/**********************************************************************************************/

/*
  Order matching engine
  Version 2
  One static hashtable + two lookup tables
  Eugene Logunov `2011
  
  Concept:
    Orderbook is implemented as a triplet of structures: a hashtable and a pair of
    lookup tables. Each of lookup tables corresponds an order side.
    
    Hashtable is static (never growing; its size is HASHTAB_SIZE); collisions are
    resolved via chaining. Furthermore, hashtable allows iteration through its nodes
    with respect to priority (which is price-time) using flink/blink_priority pointers
    of t_hashtab_node.
    
    When placing limit order - it is inserted into hashtable using order id as a key.
    So it is possible to find order in O(LOAD_FACTOR) time (required for cancel();
    LOAD_FACTOR in the worst case would be (MAX_LIVE_ORDERS / HASHTAB_SIZE), assuming
    that hash function works fine). Insertion into hashtable is O(1).
    
    As for iteration of hashtable nodes - it performs usage of lookup tables. When order
    is inserted into hashtable - the corresponding node should have flink/blink_priority
    pointers initialized (the node should be inserted before/after the latest node, which
    has the same price). Each lookup table performs mapping from price to the node,
    corresponding latest order placed at that price. Insertion/removal/find prev/find next
    are O(Log2(MAX_PRICE - MIN_PRICE)), replace/get are O(1). Since hashtable nodes are
    linked by priority - finding next node takes O(1) time. As a result, orders matching
    is very fast.
    
    Memory for hashtable nodes is allocated from static stack-based pool. Pool is a block
    of memory (large enough for worst cases - max number of orders) and a stack. Block is
    an array of structures of necessary type. When pool is initialized - stack is filled
    with pointers to elements of block. When allocating node - a pointer is popped from
    stack and returned. When deallocating node - a pointer is pushed to stack. This approach
    allows to allocate/free memory very fast (to compare with malloc()/free()); furthermore,
    memory deallocation on deinitialization is not required.
    
    Lookup table is an array-based mapping with additional helper structure - triangular
    array of counters. E.g.:
    
      lookup table (capacity = 4):
        counters:
          [x] // level 0; 1 counter
          [x x] // level 1; 2 counters
          [x x x x] // level 2; 4 counters
        values:
          [y y y y] // indexed by price values; some values may be empty
    
    This implementation uses 17-level lookup table. Counters at the last level are either
    zero, or one, depending on presence of value corresponding. Other counters are updated
    in the following way:
    
      counter[lvl - 1][idx] = counter[lvl][idx * 2] + counter[lvl][idx * 2 + 1]
    
    E.g.:
    
      counters:
        3
        2 1
        1 1 0 1
        1 0 0 1 0 0 1 0
      values:
        2 0 0 3 0 0 5 0 // example values; 0 - empty
    
    So it is possible to walk through a triangular array of counters and find next/prev
    non-empty value in O(Log2(LOOKUP_TABLE_CAPACITY)).
*/

/**********************************************************************************************/

#include "engine.h"

#ifndef NULL
  #define NULL 0
#endif

#define FALSE 0
#define TRUE 1

#define SIDE_BID 0
#define SIDE_ASK 1

/**********************************************************************************************/

// Settings for hashtables.

/*
  Number of significant bits in hash.
*/
#define HASH_BITS 11

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
  
  There is single hashtable: one node required per
  order, one node per chain sentinel, and two nodes
  for bids/asks sentinels.
  
  Nodes are allocated from a static pool.
*/
#define HASHTAB_MAX_NODES (MAX_LIVE_ORDERS + HASHTAB_SIZE + 2)

/**********************************************************************************************/

// Settings for lookup tables.

/*
  Number of levels of counters.
  
  Log2(MAX_PRICE - MIN_PRICE) + 1
*/
#define LUT_NUM_LEVELS 17

/*
  Index of last level of counters.
*/
#define LUT_LAST_LEVEL (LUT_NUM_LEVELS - 1)

/*
  Total number of counters in lookup table.
*/
#define LUT_NUM_COUNTERS ((1 << LUT_NUM_LEVELS) - 1)

/*
  Lookup table capacity.
*/
#define LUT_CAPACITY (1 << (LUT_NUM_LEVELS - 1))

/*
  Empty value for lookup table.
*/
#define LUT_EMPTY_VALUE ((t_lut_value)(0))

/**********************************************************************************************/

// Internal types.

/*
  Hash function value (masked by HASH_MASK).
*/
typedef int t_hash;

/*
  Hashtable node structure.
  
  Nodes store orderids and corresponding orders.
  Nodes with the same hash are doubly-linked using
  flink/blink_chain; furthermore, non-sentinel nodes
  are doubly-linked according to price-time priority
  using flink/blink_priority.
*/
typedef struct t_hashtab_node_internal {
  struct t_hashtab_node_internal* flink_chain;
  struct t_hashtab_node_internal* blink_chain;
  t_orderid orderid;
  struct t_hashtab_node_internal* flink_priority;
  struct t_hashtab_node_internal* blink_priority;
  t_order order;
} t_hashtab_node;

/*
  Hashtable structure.
  
  Contains array of pointers to sentinel nodes.
*/
typedef struct {
  t_hashtab_node* chains[HASHTAB_SIZE];
} t_hashtab;

/*
  Lookup table key type.
*/
typedef t_price t_lut_key;

/*
  Lookup table value type.
*/
typedef t_hashtab_node* t_lut_value;

/*
  Lookup table counter type.
*/
typedef unsigned int t_lut_counter;

/*
  Lookup table structure.
  
  level_size - size of each level (powers of 2);
  levels - pointers to first counter of each level.
  counters and values are exactly what they are.
*/
typedef struct t_lut_internal {
  int level_size[LUT_NUM_LEVELS];
  t_lut_counter* levels[LUT_NUM_LEVELS];
  t_lut_counter counters[LUT_NUM_COUNTERS];
  t_lut_value values[LUT_CAPACITY];
} t_lut;

/*
  Lookup table scan function type.
  
  Scan function searches for the nearest
  non-empty value to the left/right.
*/
typedef t_lut_value (*t_lut_scan_func)(t_lut*, t_lut_key);

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
inline void hashtab_init(t_hashtab* hashtab) {
  int i;
  for (i = 0; i < HASHTAB_SIZE; i++) {
    t_hashtab_node* node = hashtab_pool_alloc();
    node -> flink_chain = node;
    node -> blink_chain = node;
    hashtab -> chains[i] = node;
  }
}

/*
  Returns a pointer to hashtable node; the node
  is not inserted into flink/blink_priority
  double-linked list.
*/
inline t_hashtab_node* hashtab_insert(t_hashtab* hashtab, t_order* order, t_orderid orderid) {
  t_hashtab_node* node = hashtab_pool_alloc();

  t_hash hash = hashfunc_calc(orderid);
  t_hashtab_node* chain_start = hashtab -> chains[hash];
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
inline t_hashtab_node* hashtab_find(t_hashtab* hashtab, t_orderid orderid) {
  t_hash hash = hashfunc_calc(orderid);
  t_hashtab_node* chain_start = hashtab -> chains[hash];

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

// Lookup table.

/*
  Initializes internal structure of lookup table.
*/
inline void lut_init(t_lut* lut) {
  int curCounter = 0, levelSize = 1, level;
  for (level = 0; level < LUT_NUM_LEVELS; level++) {
    lut -> levels[level] = &(lut -> counters[curCounter]);
    lut -> level_size[level] = levelSize;
    curCounter += levelSize;
    levelSize <<= 1;
  }

  int i;
  for (i = 0; i < LUT_NUM_COUNTERS; i++)
    lut -> counters[i] = 0;
  for (i = 0; i < LUT_CAPACITY; i++)
    lut -> values[i] = LUT_EMPTY_VALUE;
}

/*
  Increments all counters which track given key.
  Internal function for insertion op.
*/
inline void lut_inc_counters(t_lut* lut, t_lut_key key) {
  int level;
  for (level = LUT_LAST_LEVEL; level >= 0; level--) {
    lut -> levels[level][key]++;
    key >>= 1;
  }
}

/*
  Decrements all counters which track given key.
  Internal function for removal op.
*/
inline void lut_dec_counters(t_lut* lut, t_lut_key key) {
  int level;
  for (level = LUT_LAST_LEVEL; level >= 0; level--) {
    lut -> levels[level][key]--;
    key >>= 1;
  }
}

/*
  Inserts (key; value) pair into lookup table.
*/
inline void lut_insert(t_lut* lut, t_lut_key key, t_lut_value value) {
  t_lut_value oldValue = lut -> values[key];
  lut -> values[key] = value;

  if (oldValue != LUT_EMPTY_VALUE)
    return;

  lut_inc_counters(lut, key);
}

/*
  Replaces existing value for given key with
  a new one.
*/
inline void lut_replace(t_lut* lut, t_lut_key key, t_lut_value value) {
  lut -> values[key] = value;
}

/*
  Removes value by key from lookup table.
*/
inline void lut_remove(t_lut* lut, t_lut_key key) {
  if (lut -> values[key] == LUT_EMPTY_VALUE)
    return;

  lut -> values[key] = LUT_EMPTY_VALUE;

  lut_dec_counters(lut, key);
}

/*
  Gets value by key from lookup table.
  No additional actions taken on empty value.
*/
inline t_lut_value lut_get_strict(t_lut* lut, t_lut_key key) {
  return lut -> values[key];
}

/*
  Gets value by key from lookup table.
  If there is no value associated with key -
  nearest value is returned using scan_func.
*/
inline t_lut_value lut_get(t_lut* lut, t_lut_key key, t_lut_scan_func scan_func) {
  t_lut_value value = lut_get_strict(lut, key);
  if (value == LUT_EMPTY_VALUE)
    value = scan_func(lut, key);
  return value;
}

/*
  Scans for non-empty value to the left in O(Log2(N)).
  If there is no such value - returns LUT_EMPTY_VALUE.
*/
inline t_lut_value lut_scan_left(t_lut* lut, t_lut_key key) {
  int level = LUT_LAST_LEVEL, index = key;

  do {
    level--;
    index = (index - 1) >> 1;
    if (index == -1)
      return LUT_EMPTY_VALUE;
  } while ((level >= 0) && !(lut -> levels[level][index]));

  do {
    level++;
    index = (index << 1) + 1;
    if (!(lut -> levels[level][index]))
      index--;
  } while (level < LUT_LAST_LEVEL);

  return lut -> values[index];
}

/*
  Scans for non-empty value to the right in O(Log2(N)).
  If there is no such value - returns LUT_EMPTY_VALUE.
*/
inline t_lut_value lut_scan_right(t_lut* lut, t_lut_key key) {
  int level = LUT_LAST_LEVEL, index = key;

  do {
    level--;
    index = (index + 1) >> 1;
    if (index >= (lut -> level_size[level]))
      return LUT_EMPTY_VALUE;
  } while ((level >= 0) && !(lut -> levels[level][index]));

  // special case (lut is empty; key == 0)
  // handled by loop break condition in lut_scan_left
  if (level == -1)
    return LUT_EMPTY_VALUE;

  do {
    level++;
    index <<= 1;
    if (!(lut -> levels[level][index]))
      index++;
  } while (level < LUT_LAST_LEVEL);

  return lut -> values[index];
}

/**********************************************************************************************/

// Matching engine core.

/*
  Starts of flink/blink_priority double-linked
  lists for bids and asks.
*/
t_hashtab_node* bids_sentinel;
t_hashtab_node* asks_sentinel;

/*
  Lookup tables for fast search of latest
  order at given price level.
*/
t_lut bids_lut;
t_lut asks_lut;

/*
  Hashtable for t_orderid -> t_order mapping.
*/
t_hashtab hashtab;

/*
  Initializes hashtable and lookup tables.
*/
inline void eng_init() {
  orderid_init();
  
  hashtab_pool_init();
  
  hashtab_init(&hashtab);
  
  bids_sentinel = hashtab_pool_alloc();
  bids_sentinel -> flink_priority = bids_sentinel;
  bids_sentinel -> blink_priority = bids_sentinel;
  
  asks_sentinel = hashtab_pool_alloc();
  asks_sentinel -> flink_priority = asks_sentinel;
  asks_sentinel -> blink_priority = asks_sentinel;
  
  lut_init(&bids_lut);
  lut_init(&asks_lut);
}

/*
  Removes node from flink/blink_priority
  double-linked list; maintains lookup table
  pointers to hashtable nodes; removes lookup
  table entries if necessary; finally, removes
  node from hashtable.
*/
inline void eng_remove_order(t_hashtab_node* node) {
  t_hashtab_node* prev_hashtab_node = node -> blink_priority;
  t_hashtab_node* next_hashtab_node = node -> flink_priority;
  
  t_lut* lut;
  t_hashtab_node* checked_node;
  t_price order_price = (node -> order).price;
  
  if ((node -> order).side == SIDE_BID) {
    lut = &bids_lut;
    checked_node = next_hashtab_node;
    if (lut_get_strict(lut, order_price) == node) {
      if ((checked_node == bids_sentinel) || ((checked_node -> order).price != order_price))
        lut_remove(lut, order_price);
      else
        lut_replace(lut, order_price, checked_node);
    }
  }
  else {
    lut = &asks_lut;
    checked_node = prev_hashtab_node;
    if (lut_get_strict(lut, order_price) == node) {
      if ((checked_node == asks_sentinel) || ((checked_node -> order).price != order_price))
        lut_remove(lut, order_price);
      else
        lut_replace(lut, order_price, checked_node);
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
inline void eng_cross_orders(t_order* old_order, t_order* new_order) {
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
  Iterates through orderbook side and crosses orders.
  
  Returns TRUE if order was filled (so it is not
  placed to orderbook).
*/
inline int eng_match_order(t_order* order, t_side side_to_match) {
  t_price order_price = order -> price;
  
  t_hashtab_node* first_node;
  t_hashtab_node* cur_node;
  t_hashtab_node* next_node;
  
  if (side_to_match == SIDE_BID) {
    first_node = bids_sentinel;
    cur_node = first_node -> blink_priority;
    
    while (((cur_node -> order).price <= order_price) && (cur_node != first_node)) {
      eng_cross_orders(&(cur_node -> order), order);
      
      next_node = cur_node -> blink_priority;
      if ((cur_node -> order).size == 0)
        eng_remove_order(cur_node);
      cur_node = next_node;
      
      if (order -> size == 0)
        return TRUE;
    }
  }
  else if (side_to_match == SIDE_ASK) {
    first_node = asks_sentinel;
    cur_node = first_node -> flink_priority;
    
    while (((cur_node -> order).price >= order_price) && (cur_node != first_node)) {
      eng_cross_orders(&(cur_node -> order), order);
      
      next_node = cur_node -> flink_priority;
      if ((cur_node -> order).size == 0)
        eng_remove_order(cur_node);
      cur_node = next_node;
      
      if (order -> size == 0)
        return TRUE;
    }
  }
  
  return FALSE;
}

/*
  Inserts order to hashtable; find latest
  order at the same price using lookup table;
  inserts new hashtable node into flink/blink_priority
  double-linked list; updates lookup table.
*/
inline void eng_place_order(t_order* order, t_orderid orderid, t_side side_to_place) {
  t_hashtab_node* hashtab_node = hashtab_insert(&hashtab, order, orderid);
  t_price order_price = order -> price;
  
  t_hashtab_node* prev_hashtab_node;
  t_hashtab_node* next_hashtab_node;
  
  if (side_to_place == SIDE_BID) {
    next_hashtab_node = lut_get(&bids_lut, order_price, lut_scan_right);
    lut_insert(&bids_lut, order_price, hashtab_node);
    
    if (next_hashtab_node == LUT_EMPTY_VALUE)
      next_hashtab_node = bids_sentinel;
    prev_hashtab_node = next_hashtab_node -> blink_priority;
  }
  else if (side_to_place == SIDE_ASK) {
    prev_hashtab_node = lut_get(&asks_lut, order_price, lut_scan_left);
    lut_insert(&asks_lut, order_price, hashtab_node);
    
    if (prev_hashtab_node == LUT_EMPTY_VALUE)
      prev_hashtab_node = asks_sentinel;
    next_hashtab_node = prev_hashtab_node -> flink_priority;
  }
  
  prev_hashtab_node -> flink_priority = hashtab_node;
  next_hashtab_node -> blink_priority = hashtab_node;
  hashtab_node -> flink_priority = next_hashtab_node;
  hashtab_node -> blink_priority = prev_hashtab_node;
}

/*
  Removes order if found.
*/
inline void eng_cancel_order(t_orderid orderid) {
  t_hashtab_node* node = hashtab_find(&hashtab, orderid);
  if (node)
    eng_remove_order(node);
}

/**********************************************************************************************/

// Matching engine interface.

void init() {
  eng_init();
}

void destroy() {
  // intentionally blank
}

t_orderid limit(t_order order) {
  t_orderid orderid = orderid_generate();
  if (order.side == SIDE_BID) {
    if (!eng_match_order(&order, SIDE_ASK))
      eng_place_order(&order, orderid, SIDE_BID);
  }
  else if (order.side == SIDE_ASK) {
    if (!eng_match_order(&order, SIDE_BID))
      eng_place_order(&order, orderid, SIDE_ASK);
  }
  
  return orderid;
}

void cancel(t_orderid orderid) {
  eng_cancel_order(orderid);
}

/**********************************************************************************************/
