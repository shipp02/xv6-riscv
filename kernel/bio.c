// Buffer cache.
//
// The buffer cache is a linked list of buf structures holding
// cached copies of disk block contents.  Caching disk blocks
// in memory reduces the number of disk reads and also provides
// a synchronization point for disk blocks used by multiple processes.
//
// Interface:
// * To get a buffer for a particular disk block, call bread.
// * After changing buffer data, call bwrite to write it to disk.
// * When done with the buffer, call brelse.
// * Do not use the buffer after calling brelse.
// * Only one process at a time can use a buffer,
//     so do not keep them longer than necessary.


#include "types.h"
#include "param.h"
#include "spinlock.h"
#include "sleeplock.h"
#include "riscv.h"
#include "defs.h"
#include "fs.h"
#include "buf.h"
#include <stddef.h>
#include <stdbool.h>
// Highly inspired by libintrusive 

#define assert(exp) if(!exp) {printf("%s:%d, %s\n", __FILE__, __LINE__, #exp );}

struct link_s {
  struct link_s *next;
  // Acts like a tombstone
  uint8 rm;
};

struct list_s {
  struct spinlock lk;
  struct link_s *head;
  struct link_s *tail;
};

void list_init(struct list_s *list);

void list_push_front(struct list_s *list, struct link_s *link);
void list_push_back(struct list_s *list, struct link_s *link);
// void list_insert_before(struct list_s *list, struct link_s *before, struct link_s *link);
// void list_insert_after(struct list_s *list, struct link_s *after, struct link_s *link);

struct link_s *list_pop_front(struct list_s *list);
// struct link_s *list_pop_back(struct list_s *list);

struct link_s *list_head(const struct list_s *list);
struct link_s *list_tail(const struct list_s *list);

struct link_s *list_next(const struct link_s *link);

#define list_ref(ELEMENT, TYPE, MEMBER) \
    ((TYPE *)((unsigned char *)(ELEMENT) - offsetof(TYPE, MEMBER)))

void list_init(struct list_s *list) {
  initlock(&list->lk, "bcache.lru");
  list->head = 0;
  list->tail = 0;
}

void list_push_front(struct list_s *list, struct link_s *link) {
  if (list->head) {
    link->next = list->head;
    list->head = link;
  } else {
    list->head = link;
    list->tail = link;
    link->next = 0;
  }
}

void list_push_back(struct list_s *list, struct link_s *link) {
  if (list->tail) {
      list->tail->next = link;
      link->next = 0;
      list->tail = link;
  } else {
      list->head = link;
      list->tail = link;
      link->next = 0;
  }
}


struct link_s *list_pop_front(struct list_s *list) {
    struct link_s *link = list->head;
    if (!link) return 0;
    //?? if (link->next) link->next->prev = link->prev;
    if (list->head == link) list->head = link->next;
    if (list->tail == link) list->tail = 0;
    link->next = 0;
    return link;
}

void list_remove(struct list_s *list, struct link_s *link) {
  if (!link) return;
  struct link_s* prev = list_head(list);
  while(prev != 0 && prev->next != link && prev != list_tail(list)) {
    prev = list_next(prev);
  }
  if (prev) {
    prev->next = link->next;
  }
  if (list->head == link) {
    list->head = link->next;
  }
  if (list->tail == link) {
    list->tail = prev;
  }
  link->next = 0;
}

struct link_s *list_head(const struct list_s *list) {
  return list->head;
}

struct link_s *list_tail(const struct list_s *list) {
  return list->tail;
}

struct link_s *list_next(const struct link_s *link) {
  return link->next;
}

int list_len(const struct list_s* list) {
  int n = 0;
  if(list_head(list) == 0) {
    return 0;
  }
  for(struct link_s* l = list_head(list); l!= list_tail(list); l = list_next(l)) {
    n++;
  }
  n++;
  return n;
}


struct hashnode_s {
  struct hashnode_s *next;
  uint64 keylen;
  uint64 key;
};

typedef uint64 hash_function(const void* key, uint64 keylen);
// Arbitrary constant;
#define NBUCKET 32

uint64 id_hash(const void* key, uint64 keylen) {
  return *((uint64*) key);
}

struct hashtable {
  hash_function* hash_func;
  uint64 size;
  struct hashnode_s* hashnodes[NBUCKET];
  struct spinlock bucket_lock[NBUCKET];
};
int hashtable_init(struct hashtable *table, uint64 pow2size, hash_function* hash_func);
void hashtable_destroy(struct hashtable *table);
bool hashtable_insert(struct hashtable *table, struct hashnode_s *node, void *key, uint64 keylen);
struct hashnode_s *hashtable_search(struct hashtable *table, const void *key, uint64 keylen);
void hashtable_remove(struct hashtable *table, const void *key, uint64 keylen);

static void hash_node_init(struct hashnode_s *node, void *key, uint64 keylen) {
  node->key = *(uint64*)key;
  node->keylen = keylen;
  node->next = 0;
}

static inline uint64 hash_node_bin(uint64 bins, uint64 keyhash) {
  // Fast module for bins that are powers of 2.
  return keyhash % bins ;
}

static struct hashnode_s *hash_node_find(struct hashnode_s *node, const void *key, uint64 keylen) {
  uint64 k = *(uint64*) key;
  while (node) {
    if (keylen != node->keylen) {
      node = node->next;
      continue;
    }
    if (node->key == k) return node;
    node = node->next;
  }
  return NULL;
}

int hashtable_init(struct hashtable *table, uint64 size, hash_function* hash_func) {
  // assert(pow2size < sizeof(int)*8);
  table->hash_func = hash_func;
  table->size = size;
  // table->hashnodes = kalloc( table->size);
  for(int i = 0;i<NBUCKET;i++) {
    initlock(&table->bucket_lock[i], "bcache.buckets");
  }
  return  0;
}

#define hashtable_ref(ELEMENT, TYPE, MEMBER) \
    ((TYPE *)((unsigned char *)(ELEMENT) - offsetof(TYPE, MEMBER)))

void hashtable_destroy(struct hashtable *table) {
  table->hash_func = 0;
  table->size = 0;
  // free(table->hashnodes);
}

bool hashtable_insert(struct hashtable *table, struct hashnode_s *node, void *key, uint64 keylen) {
  hash_node_init(node, key, keylen);
  uint64 hash = table->hash_func(&node->key, node->keylen);
  uint64 bin = hash_node_bin(table->size, hash);
  acquire(&table->bucket_lock[bin]);
  struct hashnode_s *head = table->hashnodes[bin];
  if (!head) {
    table->hashnodes[bin] = node;
    release(&table->bucket_lock[bin]);
    return true;
  }
  struct hashnode_s *find = hash_node_find(head, &node->key, node->keylen);
  if (find) {
    // printf("Found existing node with key: %p\n", *(uint64*)key);
    // panic("Found exisiting node");
    release(&table->bucket_lock[bin]);
    return false;
  }
  table->hashnodes[bin] = node;
  node->next = head;
  release(&table->bucket_lock[bin]);
  return true;
}

struct hashnode_s *hashtable_search(struct hashtable *table, const void *key, uint64 keylen) {
    uint64 hash = table->hash_func(key, keylen);
    uint64 bin = hash_node_bin(table->size, hash);
    acquire(&table->bucket_lock[bin]);
    struct hashnode_s *node = table->hashnodes[bin];
    struct hashnode_s *res = hash_node_find(node, key, keylen);
    release(&table->bucket_lock[bin]);
    return res;
}

void hashtable_remove(struct hashtable *table, const void *key, uint64 keylen) {
  uint64 hash = table->hash_func(key, keylen);
  uint64 bin = hash_node_bin(table->size, hash);
  acquire(&table->bucket_lock[bin]);
  struct hashnode_s *current = table->hashnodes[bin];
  struct hashnode_s *prev = 0;
  while (current) {
    if (keylen != current->keylen) {
      prev = current;
      current = current->next;
      continue;
    }
    uint64 k = *(uint64*) key;
    if (current->key == k) {
      if (prev) {
        prev->next = current->next;
      }
      else {
        table->hashnodes[bin] = current->next;
      }
      current->next = 0;
      release(&table->bucket_lock[bin]);
      return;
    }
    prev = current;
    current = current->next;
  }
  release(&table->bucket_lock[bin]);
}

struct buf_hash_lru {
  struct buf b;
  struct hashnode_s hash_node;
  struct link_s link;
};
struct {
  struct spinlock lock;
  struct buf_hash_lru buf[NBUF];
  struct hashtable buf_table;
  struct list_s lru;

  // Linked list of all buffers, through prev/next.
  // Sorted by how recently the buffer was used.
  // head.next is most recent, head.prev is least.
  struct buf head;
} bcache;


void
binit(void)
{
  struct buf_hash_lru *b;

  initlock(&bcache.lock, "bcache");
  list_init(&bcache.lru);
  hashtable_init(&bcache.buf_table, NBUCKET, id_hash);

  // Create linked list of buffers
  // bcache.head.prev = &bcache.head;
  // bcache.head.next = &bcache.head;
  for(b = bcache.buf; b < bcache.buf+NBUF; b++){
    list_push_front(&bcache.lru, &b->link);
    initsleeplock(&b->b.lock, "buffer");
    initlock(&b->b.data_lk, "bcache.data");
  }
  assert((list_len(&bcache.lru) == NBUF));
}

// Look through buffer cache for block on device dev.
// If not found, allocate a buffer.
// In either case, return locked buffer.
static struct buf*
bget(uint dev, uint blockno)
{
  struct buf_hash_lru *b;
  uint64 try_count = 0;


  uint64 key = ((((uint64) dev) << 32) | blockno);
  // Is the block already cached?
retry:
  struct hashnode_s* n = hashtable_search(&bcache.buf_table, &key, 8);
  if(n) {
    b = hashtable_ref(n, struct buf_hash_lru, hash_node);
    struct buf* buf = &b->b;
    if(buf->dev == dev && buf->blockno == blockno){
      acquire(&buf->data_lk);
      buf->refcnt++;
      release(&buf->data_lk);
      acquiresleep(&buf->lock);
      return buf;
    }
    else if (buf!= 0) {
      printf("Found wrong block: found %d, %d looking for %d, %d\n", buf->dev, buf->blockno, dev, blockno);
      panic("Found wrong block");
    }

  } else {
    // Not cached.
    // Recycle the least recently used (LRU) unused buffer.
    acquire(&bcache.lock);
    for(struct link_s* l = list_head(&bcache.lru); l != list_tail(&bcache.lru); l = list_next(l)){
      b = list_ref(l, struct buf_hash_lru, link);
      if(b->b.refcnt == 0) {
        list_remove(&bcache.lru, &b->link);
        release(&bcache.lock);

        uint64 key = ((((uint64) dev) << 32) | blockno);

        if(hashtable_insert(&bcache.buf_table, &b->hash_node, &key, 8)) {
          struct buf* buf = &b->b;
          acquire(&buf->data_lk);
          buf->dev = dev;
          buf->blockno = blockno;
          buf->valid = 0;
          buf->refcnt = 1;
          release(&buf->data_lk);
          acquiresleep(&buf->lock);
          return buf;
        } else {
          try_count++;
          goto retry;
        }
      }
    }
    release(&bcache.lock);
    panic("bget: no buffers");

  }

  return 0;
}

// Return a locked buf with the contents of the indicated block.
struct buf*
bread(uint dev, uint blockno)
{
  struct buf *b;

  b = bget(dev, blockno);
  if(!b->valid) {
    virtio_disk_rw(b, 0);
    b->valid = 1;
  }
  return b;
}

// Write b's contents to disk.  Must be locked.
void
bwrite(struct buf *b)
{
  if(!holdingsleep(&b->lock))
    panic("bwrite");
  virtio_disk_rw(b, 1);
}

// Release a locked buffer.
// Move to the head of the most-recently-used list.
void
brelse(struct buf *b)
{
  if(!holdingsleep(&b->lock))
    panic("brelse");

  releasesleep(&b->lock);
  struct buf_hash_lru *b_ds = (struct buf_hash_lru*) b;

  acquire(&b->data_lk);
  b->refcnt--;
  if (b->refcnt == 0) {
    // no one is waiting for it.
    // Probably need to do something for the LRU.
    release(&b->data_lk);

    hashtable_remove(&bcache.buf_table, &b_ds->hash_node.key, 8);
    acquire(&bcache.lock);
    list_push_back(&bcache.lru, &b_ds->link);
    release(&bcache.lock);

  } else {
    release(&b->data_lk);
  }
}

void
bpin(struct buf *b) {
  acquire(&b->data_lk);
  b->refcnt++;
  release(&b->data_lk);
}

void
bunpin(struct buf *b) {
  acquire(&b->data_lk);
  b->refcnt--;
  release(&b->data_lk);
}


#undef assert