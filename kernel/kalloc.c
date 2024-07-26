// Physical memory allocator, for user processes,
// kernel stacks, page-table pages,
// and pipe buffers. Allocates whole 4096-byte pages.

#include "types.h"
#include "param.h"
#include "memlayout.h"
#include "spinlock.h"
#include "riscv.h"
#include "defs.h"

void freerange(void *pa_start, void *pa_end);
void kfree_core(void*, int);

extern char end[]; // first address after kernel.
                   // defined by kernel.ld.

struct run {
  struct run *next;
};


int run_length(struct run* r) {
  int x = 0;
  struct run* curr = r;
  while(curr != 0) {
    x++;
    curr = curr->next;
  }
  return x;
}

// struct {
//   struct spinlock lock;
//   struct run *freelist;
// } kmem;

struct {
  struct spinlock lock;
  struct run* freelist;
  int n;
} core_mem[NCPU];

void
kinit()
{
  // initlock(&kmem.lock, "kmem");
  for(int i = 0;i<NCPU;i++) {
    initlock(&core_mem[i].lock, "kmem");
  }
  freerange(end, (void*)PHYSTOP);
}

void kfree_core(void* pa, int cpu) {
  acquire(&core_mem[cpu].lock);
  struct run *r;

  if(((uint64)pa % PGSIZE) != 0 || (char*)pa < end || (uint64)pa >= PHYSTOP)
    panic("kfree");

  // Fill with junk to catch dangling refs.
  memset(pa, 1, PGSIZE);

  r = (struct run*)pa;

  r->next = core_mem[cpu].freelist;
  core_mem[cpu].freelist = r;
  core_mem[cpu].n++;
  release(&core_mem[cpu].lock);
}

#define assert(exp) if(!exp) {printf("%s:%d, %s\n", __FILE__, __LINE__, #exp );}
void
freerange(void *pa_start, void *pa_end)
{
  char *p;
  char *p_start_last;
  int core = 0;
  p = (char*)PGROUNDUP((uint64)pa_start);
  p_start_last = p;
  const uint64 each = ((uint64)pa_end - (uint64)p)/NCPU;
  for(; p + PGSIZE <= (char*)pa_end; p += PGSIZE) {
    if(p-p_start_last > each) {
      p_start_last = p;
      // assert((run_length(core_mem[core].freelist) == core_mem[core].n));
      core++;
    }
    kfree_core(p, core);
  }
}


// Free the page of physical memory pointed at by pa,
// which normally should have been returned by a
// call to kalloc().  (The exception is when
// initializing the allocator; see kinit above.)

void kfree(void* pa) {
  push_off();
  int cpu = cpuid();
  pop_off();
  acquire(&core_mem[cpu].lock);
  struct run *r;

  if(((uint64)pa % PGSIZE) != 0 || (char*)pa < end || (uint64)pa >= PHYSTOP)
    panic("kfree");

  // Fill with junk to catch dangling refs.
  memset(pa, 1, PGSIZE);

  r = (struct run*)pa;

  r->next = core_mem[cpu].freelist;
  core_mem[cpu].freelist = r;
  core_mem[cpu].n++;
  // assert((run_length(core_mem[cpu].freelist) == core_mem[cpu].n));
  release(&core_mem[cpu].lock);
}

int min(int a, int b) {
  if(a>b) {
    return b;
  } else {
    return a;
  }
}
int max(int a, int b) {
  if(a>b) {
    return a;
  } else {
    return b;
  }
}

// Allocate one 4096-byte page of physical memory.
// Returns a pointer that the kernel can use.
// Returns 0 if the memory cannot be allocated.
void *
kalloc(void)
{
  struct run *r = 0;
  push_off();
  int cpu0 = cpuid();;
  pop_off();

  acquire(&core_mem[cpu0].lock);
  r = core_mem[cpu0].freelist;
  if(r) {
    core_mem[cpu0].freelist = r->next;
    core_mem[cpu0].n--;
    release(&core_mem[cpu0].lock);
  } 
  else {
    release(&core_mem[cpu0].lock);
    for(int i = cpu0+1;i<cpu0+NCPU;i++) {
      int cpu = i%NCPU;
      acquire(&core_mem[min(cpu, cpu0)].lock);
      acquire(&core_mem[max(cpu, cpu0)].lock);
      struct run *r2 = 0;
      r2 = core_mem[cpu].freelist;
      // assert((run_length(core_mem[cpu].freelist) == core_mem[cpu].n));
      if(r2 && core_mem[cpu].n > 1) {
        const int count =core_mem[cpu].n/2;
        // printf("kalloc: Stealing %d pages from %d\n", count, cpu);
        struct run* prev = r2;
        for(int j = 0;j<count;j++) {
          prev = core_mem[cpu].freelist;
          core_mem[cpu].freelist = core_mem[cpu].freelist->next;
          core_mem[cpu].n--;
        }
        prev->next = 0;
        core_mem[cpu0].freelist = r2;
        core_mem[cpu0].n = count;
        // assert((run_length(core_mem[cpu0].freelist) == core_mem[cpu0].n));
        release(&core_mem[max(cpu, cpu0)].lock);
        release(&core_mem[min(cpu, cpu0)].lock);
        break;
      } else {
        release(&core_mem[max(cpu, cpu0)].lock);
        release(&core_mem[min(cpu, cpu0)].lock);
      }
    }
    acquire(&core_mem[cpu0].lock);
    r = core_mem[cpu0].freelist;
    if(r) {
      core_mem[cpu0].freelist = r->next;
      core_mem[cpu0].n--;
    }
    release(&core_mem[cpu0].lock);
  }

  if(r)
    memset((char*)r, 5, PGSIZE); // fill with junk
  return (void*)r;
}

#undef assert