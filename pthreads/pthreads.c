/* ========================================================================== */
/* Descent Theorem in octagonal tilings; 
   Written by Max Hutchinson, jan 2010 
*/

/* ========================================================================== */
/* Header */

/* Includes */
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <mpfr.h>
#include <gmp.h>
#include <unistd.h>

/* Macro Definitions */
#define NUM_THREADS 1
#define HASH_SIZE 134217728 /* Size of the Hash Table, must be power of 2 */
#define HASH_MOD 0x7FFFFFFl /* Mask for the hash table, should be hex(HASH_SIZE-1) */
#define MAX_CHILD 10
#define MAX_DESC 5
#define MAX_TOT 15

//#define FOR_DISPLAY

/* Defines the interval for status updatse.
   Remove to disable status updates */
#define STATUS 1

/* Macro Functions */
#define BITSVAL(X,Y) (((X)>>(2*(Y)))&0x3)

/* Type definitions */
typedef signed char vec[3];
typedef unsigned long ptype;

typedef struct vertex{
  ptype path;
  unsigned char child_counter;
  unsigned char num_child;
  vec faces[MAX_CHILD];
/* This part will be copied directly*/
  mpz_t vals[MAX_CHILD][MAX_DESC];
  struct vertex *hnext; // path backed by doubly linked list
  struct vertex *hlast;
  struct vertex *qnext; // queue is singly linked
  sem_t mutex;
  unsigned long hash_code;
} vertex;

/* Function Prototypes */
mpz_t* W(int a_in, int b_in, int c_in, int d_in);
int seed_graph(void);
int process_vertex(vertex* v);
void centerFace(vec centre, ptype parent, int flip);
inline int Order(vec Vi, vec Vf);
void* thread_work(void* vargp);
vertex* get(ptype path);
int add(vertex* to_add);
void* updater_thread(void* vargp);
long int binom(int a, int b);
unsigned long hash(ptype n);
void checkpoint_read(void);
void checkpoint_write(void);

/* Global Scope Variables */
/* Inputs */
int K, L, M, N, tot, num_hex_tiles;
long unsigned int root_path;

/* Data */
/* path table backed by a linked list */
vertex **head_by_path;
vertex **tail_by_path;
sem_t *hmutex;

/* Queue */
vertex *queue_head;
vertex *queue_tail;
vertex *root = NULL;
sem_t qmutex;
sem_t qsize;

/* Counter Data */
long unsigned int counter = 0;
sem_t cmutex;
unsigned long checkpoint_period = 10000000;

/* I/O */
char checkpoint_bin_name[100] = "checkpoint.bin";
char checkpoint_desc_name[100] = "checkpoint.desc";
FILE* update_file;
int restart = 0;

/* Thread stuff */
pthread_t tids[NUM_THREADS+1];

/* Arrays that will contain the partitions that will build the 
   3->1 type tilings */
int p[3];

/* Precomputer powers of three for the hash function */
long unsigned int powers[MAX_TOT];

int main(int argc, char* argv[]){
  /* Defaults for I/O */
  update_file = stderr;


  // Parse the args.
  int s;
  while ((s = getopt (argc, argv, "c:u:p:r::")) != -1) switch (s){
    case 'c':
      sprintf(checkpoint_bin_name, "%s.bin", optarg);
      sprintf(checkpoint_desc_name, "%s.desc", optarg);
      break;
/* This is currently broken...
    case 'u':
      update_file = fopen(optarg, "w");
      break;
*/
    case 'p':
      sscanf(optarg, "%lu", &checkpoint_period);
      break;
    case 'r':
      restart = 1;
      break;
  }

  /* Allocate some stuff */
  head_by_path = (vertex**) malloc(HASH_SIZE * sizeof(vertex*));
  tail_by_path = (vertex**) malloc(HASH_SIZE * sizeof(vertex*));
  hmutex = (sem_t*) malloc(HASH_SIZE * sizeof(sem_t));

  int i;
  for (i = 0; i < MAX_TOT; i++)
    powers[i] = pow(3,i);

  mpz_t *ans = W(5,5,5,5);
  
  /* Compute area and number of tiles analytically */
  double area = (K + L/sqrt(2.) + N/sqrt(2.))*(M + L/sqrt(2.) + N/sqrt(2.)) - L*L/2. - N*N/2.;
  unsigned long int num_tiles = K*M + L*N + (K+M)*(L+N);
  
  /* All sorts of messy stuff to use arbitrary precision... */
  mpfr_t ent, ned, aed;
  mpfr_init(ent); mpfr_init(ned); mpfr_init(aed);
  mpfr_set_z(ent, *ans, GMP_RNDN);
  
  mpfr_log(ent, ent, GMP_RNDN);
  mpfr_div_ui(ned, ent, num_tiles, GMP_RNDN);
  mpfr_div_d(aed, ent, area, GMP_RNDN);
  
  /* NED: Entropy Density by Tile, AED: Entropy Density by Area */
  mpfr_printf("W(%d,%d,%d,%d) = %Zd \n", K, L, M, N, *ans);
  mpfr_printf("NED: %25.20Rf \t Num Tiles: %lu \n", ned, num_tiles);
  mpfr_printf("AED: %25.20Rf \t Area: %25.20lf \n", aed, area);

  return EXIT_SUCCESS;
}

mpz_t* W(int a_in, int b_in, int c_in, int d_in){
  int i, j;
  mpz_t *WW;
  mpz_t big_tmp;
  mpz_init(big_tmp);
  vertex* ptr;
  
  /* Initialize Global Scope */
  WW = (mpz_t*) malloc(MAX_DESC * sizeof(mpz_t));
  p[0] = 1; p[1] = 0; p[2] = 2; //Permutation found in CentreFacette

  K = a_in; L = b_in; M = c_in; N = d_in;
  tot = K + L + M;
  num_hex_tiles = K*L + K*M + L*M; 
  
  /* Set up Locks */
  sem_init(&qmutex, 0, 1);
  sem_init(&qsize, 0, 0);
  sem_init(&cmutex, 0, 1);
  for (i = 0; i < HASH_SIZE; i++)
    sem_init(hmutex + i, 0, 1);
  
  /* Create the two base verticies in the graph */
  if (restart)
    checkpoint_read();
  else
    seed_graph();

  /* Compute the root path apriori */
  root_path = 0;
  for (i = 0; i < M; i++)
    root_path += 2 * pow(4,i);
  for (i = M; i < M+L; i++)
    root_path += 1 * pow(4,i);

#ifdef STATUS    
  pthread_create(tids+NUM_THREADS, NULL, updater_thread, NULL);
#endif

  while (root == NULL){
    /* Spawn the threads */
    for (i = 0; i < NUM_THREADS; i++)
      pthread_create(tids + i, NULL, thread_work, &checkpoint_period);

    /* Join the threads */
    for (i = 0; i < NUM_THREADS; i++)
      pthread_join(tids[i], NULL);
      
    checkpoint_write();
  }

#ifdef STATUS
  pthread_join(tids[NUM_THREADS], NULL);
#endif

  
  /* Sum the children of the last vertex */
  mpz_t ajs[MAX_DESC];
  for (i = 0; i < MAX_DESC; i++){
    mpz_init_set_si(ajs[i], 0);
    for (j = 0; j < root->child_counter; j++) {
      mpz_add(ajs[i], root->vals[j][i], ajs[i]);
    }
    gmp_printf("%d\t%d\t%d\t%d\t%Zd\n",K, L, M, i, ajs[i]);
  } 
  printf("\n"); 
  
  /* Application of Octagonal counting formula */
  for (i=1; i < MAX_DESC; i++){
    mpz_init_set_si(WW[i], 0);
    for (j = 0; j <= i; j++){
      mpz_bin_uiui(big_tmp, i + num_hex_tiles - j, num_hex_tiles);
      mpz_mul(big_tmp, big_tmp, ajs[j]);
      mpz_add(WW[i], WW[i], big_tmp);
    }
    //gmp_printf("W(%d,%d,%d,%d) = %Zd \n", K, L, M, i, WW[i]);
  }

  /* Remove the vertex from the path table */
  if (root->hlast != NULL)
    root->hlast->hnext = root->hnext;
  else
    head_by_path[root->hash_code&HASH_MOD] = root->hnext;
  if (root->hnext != NULL)
    root->hnext->hlast = root->hlast;
  else
    tail_by_path[root->hash_code&HASH_MOD] = root->hlast;

  queue_head = NULL; queue_tail = NULL;

  /* Free the vertex's memory (Hooray!) */
  for (i = 0; i < MAX_CHILD; i++)
    for (j = 0; j < MAX_DESC; j++)
      mpz_clear(root->vals[i][j]);
  free(root);

  return WW + N;
}

int seed_graph(){
  int i, j;
  
  /* Seed first vertex */
  vertex* ptr = (vertex*) malloc(sizeof(vertex));
  sem_init(&(ptr->mutex), 0, 1);
  
  /* First vertexs have one child */
  ptr->num_child = 1;
  ptr->child_counter = 1;
  
  /* path has single flip from fully ordered */
  ptr->path = 0;
  for (i = 0; i < K-1; i++)
    ptr->path |= 0<<(2*i);
  ptr->path |= 1l<<(2*(K-1));
  ptr->path |= 0l<<(2*K);
  for (i = K+1; i < K+L; i++)
    ptr->path |= 1l<<(2*i);
  for (i = K+L; i < tot; i++)
    ptr->path |= 2l<<(2*i);

  ptr->hash_code = hash(ptr->path);

  /* Analytically know this */
  ptr->faces[0][0] = -1; ptr->faces[0][1] = 2*K - 1; ptr->faces[0][2] = 0;
  
  /* Define this */
  mpz_init_set_si(ptr->vals[0][0], 1);
  for (i = 1; i < MAX_DESC; i++)
    mpz_init2(ptr->vals[0][i], 8);
    
  /* Add to data structures */
  add(ptr);
    
  /* Seed second vertex */
  ptr = (vertex*) malloc(sizeof(vertex));
  sem_init(&(ptr->mutex), 0, 1);
  
  /* First vertexs have one child */
  ptr->num_child = 1;
  ptr->child_counter = 1;
  
  /* path has single flip from fully ordered */
  ptr->path = 0;
  for (i = 0; i < K; i++)
    ptr->path |= 0l<<(2*i);
  for (i = K; i < K+L-1; i++)
    ptr->path |= 1l<<(2*i);
  ptr->path |= 2l<<(2*(K+L-1));
  ptr->path |= 1l<<(2*(K+L));
  for (i = K+L+1; i < tot; i++)
    ptr->path |= 2l<<(2*i);

  ptr->hash_code = hash(ptr->path);
  
  /* Analytically know this */
  ptr->faces[0][0] = -2*L+1; ptr->faces[0][1] = 2*K; ptr->faces[0][2] = 1;
  
  /* Define this */
  mpz_init_set_si(ptr->vals[0][0], 1);
  for (i = 1; i < MAX_DESC; i++)
    mpz_init2(ptr->vals[0][i], 8);
    
  /* Add to data structures */
  add(ptr);

  return EXIT_SUCCESS;
}

/* Apply the vertex's contributions to the running values of its parents */
int process_vertex(vertex* v){
  int flip, i, j;
  ptype pparent;
  vertex* parent;
  
  /* Iterate through possible flips */
  for (flip = 0; flip < tot-1; flip++){
    /* Select only valid flips */
    if (BITSVAL(v->path,flip) >= BITSVAL(v->path,flip+1))
      continue;
    
    /* Construct the flip */
    pparent = v->path;
    pparent &= ~(0xFl<<(2*flip));
    pparent |= BITSVAL(v->path,flip+1)<<(2*flip);
    pparent |= BITSVAL(v->path,flip)<<(2*flip+2);
    
    /* Search for matching parent */
    parent = get(pparent);
    sem_wait(&(parent->mutex));
    
      /* Find the face */
      centerFace(parent->faces[parent->child_counter], pparent, flip);
    
      /* Run the decent algorithm */
      for (i = 0; i < v->child_counter; i++){
        if (Order(parent->faces[parent->child_counter], v->faces[i])){
          for (j = 0; j < MAX_DESC; j++)
            mpz_add(parent->vals[parent->child_counter][j], parent->vals[parent->child_counter][j], v->vals[i][j]);
        }else{
          for (j = 0; j < MAX_DESC-1; j++)
            mpz_add(parent->vals[parent->child_counter][j+1], parent->vals[parent->child_counter][j+1], v->vals[i][j]);
        }
      }  
    
      /* Book keeping */
      (parent->child_counter)++;
    sem_post(&(parent->mutex));
    
  }

  return EXIT_SUCCESS;
}

/* Compute twice the coordinates of the face bordered by the flip */
void centerFace(vec centre, ptype parent, int flip){
  int jj;
  vec tmp;
  
  for (jj = 0; jj < 3; jj++)
    tmp[jj] = 0;
  for (jj = 0; jj < flip; jj++)
    tmp[BITSVAL(parent,jj)] += 2; 
  
  tmp[BITSVAL(parent,flip)]++;
  tmp[BITSVAL(parent,flip+1)]++;
  
  for (jj = 0; jj < 3; jj++)
    centre[jj] = tmp[p[jj]];
  
  centre[0] = -centre[0];
}

/* Assigns a standard ordering to points in 3D space */
inline int Order(vec Vi, vec Vf){
  return ((Vi[2]>Vf[2]) || ((Vi[2]==Vf[2]) && (Vi[1]<Vf[1])) ||
      ((Vi[2]==Vf[2]) && (Vi[1]==Vf[1]) && 
       (((Vi[2]%2) && (Vi[0]>Vf[0])) || (!(Vi[2]%2) && (Vi[0]<Vf[0])))));
}

/* Outer thread working routine */
void* thread_work(void* vargp){
  vertex* ptr = NULL;
  int i, j;
  unsigned long count = *((unsigned long*) vargp);
  
  
  while (count > 0 && root == NULL){
    /* Decrement the counter (blocking if the queue is emtpy) */
    sem_wait(&qsize);
    /* Lock the queue and take the first node out of it */
    sem_wait(&qmutex);
      if (queue_head != NULL){
        ptr = queue_head;
        queue_head = queue_head->qnext;
      }
    sem_post(&qmutex);
    
    /* Check to see if you're done... */
    if (ptr->path == root_path){
      root = ptr;
      break;
    } 
    /* Wait until all of the node's children have been processed */
    while (ptr->child_counter != ptr->num_child)
      i = i;
    
    /* Process the node */
    process_vertex(ptr);
    
    /* Remove the vertex from the hash tables */
    sem_wait(hmutex + (ptr->hash_code&HASH_MOD));
      /* Remove the vertex from the path table */
      if (ptr->hlast != NULL)
        ptr->hlast->hnext = ptr->hnext;
      else
        head_by_path[ptr->hash_code&HASH_MOD] = ptr->hnext;
      if (ptr->hnext != NULL)
        ptr->hnext->hlast = ptr->hlast;
      else
        tail_by_path[ptr->hash_code&HASH_MOD] = ptr->hlast;
    sem_post(hmutex + (ptr->hash_code&HASH_MOD));    
    
    /* Free the vertex's memory (Hooray!) */
    for (i = 0; i < MAX_CHILD; i++)
      for (j = 0; j < MAX_DESC; j++)
        mpz_clear(ptr->vals[i][j]);
    free(ptr);
    ptr = NULL;
    count--;
  }

  return NULL;
}

/* Get a vertex out of the pathtable given a path */
vertex* get(ptype path){
  int i, j;
  
  int hash_code = hash(path);

  sem_wait(hmutex + (hash_code&HASH_MOD));
    vertex* ptr = head_by_path[hash_code&HASH_MOD];
    while (ptr != NULL){
      if (ptr->path == path){
        //printf("path hit \n");
        sem_post(hmutex + (hash_code&HASH_MOD));
        return ptr;
      }
      ptr = ptr->hnext;
    }
  
    /* Create one if it doesn't yet exhist */
    ptr = (vertex*) malloc(sizeof(vertex));
    sem_init(&(ptr->mutex), 0, 1);
    ptr->path = path;
    ptr->hash_code = hash_code;
    ptr->child_counter = 0;
    ptr->num_child = 0;
    for (i = 0; i < tot-1; i++)
      if (BITSVAL(path,i) > BITSVAL(path,i+1))
        ptr->num_child++;

    for (i = 0; i < MAX_CHILD; i++)
      for (j = 0; j < MAX_DESC ; j++)
        mpz_init2(ptr->vals[i][j], 8);
          
    add(ptr);
    
  sem_post(hmutex + (hash_code&HASH_MOD));
  
  return ptr;
}

/* Add a vertex to the path table and queue */
int add(vertex* to_add){
  int i;
  
  /* Add to path table */
  to_add->hnext = NULL;
  if (head_by_path[to_add->hash_code&HASH_MOD] == NULL){
    head_by_path[to_add->hash_code&HASH_MOD] = to_add;
    to_add->hlast = NULL;
  } else{
    tail_by_path[to_add->hash_code&HASH_MOD]->hnext = to_add;
    to_add->hlast = tail_by_path[to_add->hash_code&HASH_MOD];
  } 
  tail_by_path[to_add->hash_code&HASH_MOD] = to_add;

  /* Add to queue */  
  sem_wait(&qmutex);
    if (queue_head == NULL)
      queue_head = to_add;
    else 
      queue_tail->qnext = to_add;
  
    queue_tail = to_add;
    to_add->qnext = NULL;
  sem_post(&qmutex);
  
  /* Increment the counter */
  sem_post(&qsize);

#ifdef STATUS  
  /* Increment the counter */
  sem_wait(&cmutex);
    counter++;
  sem_post(&cmutex);
#endif

  return EXIT_SUCCESS;
}

#define BAR_WIDTH 50

void* updater_thread(void* vargp){
  int i;
  unsigned long int total_size = 1;
  total_size *= binom(K+L,K);
  total_size *= binom(K+L+M,M);
  unsigned long int local_counter = 0;
  clock_t start; 
  float time_ish, tdiff, etime;
  int time_width, count_width;
  unsigned long int ndiff;
  
  start = clock();

  sem_wait(&cmutex);
    local_counter = counter;
  sem_post(&cmutex);
  
  while (root == NULL){
    sleep(STATUS);
    sem_wait(&cmutex);
      ndiff = counter - local_counter;
      local_counter = counter;
    sem_post(&cmutex);
    
    tdiff = ((float) (clock() - start))/ CLOCKS_PER_SEC - time_ish;
    time_ish = ((float) (clock() - start))/ CLOCKS_PER_SEC;
    
    if (ndiff != 0)
      etime = (total_size-local_counter)/ndiff*tdiff + time_ish;
    else
      etime += tdiff;

    count_width = log10(total_size)+1;
    time_width = log10(etime)+4;

    fprintf(update_file, "\r[%*ld/%*ld] |", count_width, local_counter, count_width, total_size);
    for (i=1; i <= BAR_WIDTH; i++)
      if (((int) BAR_WIDTH * local_counter / total_size) > i)
        fprintf(update_file, "=");
      else
        fprintf(update_file, " ");
    fprintf(update_file, "| [%*.2fs/%*.2fs]  Rate: %d Nodes/s", time_width, time_ish, time_width, etime, (int) (ndiff/tdiff));

#ifndef FOR_DISPLAY
    fprintf(update_file, "\n");
#endif
  }
  fprintf(update_file, "\n");
  
  fclose(update_file);
  

  return NULL;
}

/* Simple Math Helper Functions */
long int binom(int a, int b){
  if (a < 0 || b < 0)
    return 0;
    
  if (b > a)
    return 0;

  int it;    
  long int tmp = 1;
  
  for (it = 0; it < b; it++)
    tmp *= (a-it);
          
  for (it = b; it > 1; it--)
    tmp /= it;

  return tmp;
}

unsigned long hash(ptype path){
  unsigned long val = 0;
  int i;
  for (i = 0; i < tot; i++){
    val += powers[i] * BITSVAL(path, i);
  }
  return val;
}

static int checkpoint_index = 0;
void checkpoint_write(void){
  /* Move the old checkpoint to prevent loss of data in case of interuption during checkpoint */
  char buffer[1000];
  sprintf(buffer, "mv -u %s %s%d \n", checkpoint_bin_name, checkpoint_bin_name, checkpoint_index);
  system(buffer);
  sprintf(buffer, "mv -u %s %s%d \n", checkpoint_desc_name, checkpoint_desc_name, checkpoint_index);
  system(buffer);
  checkpoint_index++;

  /* Open checkpoint files */
  FILE* fbin = fopen(checkpoint_bin_name, "wb");  
  int i, j;
  /* Size of the portion of the struct that is actually being written */
  size_t struct_size = sizeof(ptype) + 2* sizeof(unsigned char) + MAX_CHILD * sizeof(vec);
  
  unsigned long int count = 0;
  vertex* ptr = queue_head;
  
  /* Walk through the queue */
  while(ptr != NULL){
    count++;
    /* Write the first part of the vertex */
    fwrite(ptr, struct_size, 1, fbin);
    /* Write the values */
    for (i = 0; i < MAX_CHILD; i++)
      for (j = 0; j < MAX_DESC; j++)
        mpz_out_raw(fbin, ptr->vals[i][j]);
    ptr = ptr->qnext;
  }
  
  FILE* fdesc = fopen(checkpoint_desc_name, "w");
  /* WRite a descriptor file with the counter and the size of the queue */
  fprintf(fdesc, "%lu %lu\n", count, counter-count);
  
  /* Close files */
  fclose(fbin);
  fclose(fdesc);
  
  fprintf(update_file, "Checkpointed \n");
}

/* Read data from a checkpoint file */
void checkpoint_read(void){
  /* Open checkpoint files */
  FILE* fbin = fopen(checkpoint_bin_name, "rb");
  FILE* fdesc = fopen(checkpoint_desc_name, "r");
  
  int i, j, k;
  /* Size of the portion of the struct that is actually being read */
  size_t struct_size = sizeof(ptype) + 2* sizeof(unsigned char) + MAX_CHILD * sizeof(vec);
  unsigned long count;
  
  /* Read the size of the queue and the value of the counter */
  fscanf(fdesc, "%lu %lu\n", &count, &counter);
  
  /* Malloc and read each vertex */
  vertex* ptr;
  for (i = 0; i < count; i++){
    ptr = malloc(sizeof(vertex));
    fread(ptr, struct_size, 1, fbin);
    for (j = 0; j < MAX_CHILD; j++){
      for (k = 0; k < MAX_DESC; k++){
        mpz_inp_raw(ptr->vals[j][k], fbin);
      }
    }
    /* Fill in missing values */
    ptr->hash_code = hash(ptr->path);
    sem_init(&(ptr->mutex), 0, 1);
    /* Add it to the queue/hash table */
    add(ptr);
  }
  
  /* Close files */
  fclose(fbin);
  fclose(fdesc);
}


