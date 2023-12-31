#include <byteswap.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#define BLOCK_COUNT_64 (512*1024)
#define BLOCK_COUNT_32 (1024*1024)
#define BLOCK_COUNT_16 (2048*1024)

/* This file contains various implementations of fast byteswapping
   routines.  The main entry point, fast_byteswap, is the only one you
   should need, and it should be modified to use whatever method is
   fastest on your architecture.  

   In all cases, the routines return 1 on success and 0 on failure.
   They only fail if your data is non-aligned.  All routines require
   that arrays of N-bit data be N-bit aligned.  If they are not, an
   error will be sent to stderr and the routine will return non-zero.
   To silence the error message, call fast_byteswap_errors(0).  */

static int send_errors; /* if non-zero, warn about non-aligned pointers */

void fast_byteswap_errors(int flag) { 
  send_errors=flag;
}

/**********************************************************************/
/* Simple single-value loops                                          */
/**********************************************************************/

static int simple_swap_64(void *data,size_t len) {
  size_t i;
  uint64_t *udata;
  if( ((size_t)data)&0x5 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 64-bit integer is not 64-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  udata=data;
  for(i=0;i<len;i++)
    udata[i]= 
      ( (udata[i]>>56)&0xff ) |
      ( (udata[i]>>40)&0xff00 ) |
      ( (udata[i]>>24)&0xff0000 ) |
      ( (udata[i]>>8) &0xff000000 ) |
      ( (udata[i]<<8) &0xff00000000 ) |
      ( (udata[i]<<24)&0xff0000000000 ) |
      ( (udata[i]<<40)&0xff000000000000 ) |
      ( (udata[i]<<56)&0xff00000000000000 );
  return 1;
}

static int simple_swap_32(void *data,size_t len) {
  size_t i;
  uint32_t *udata;
  if( ((size_t)data)&0x3 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 32-bit integer is not 32-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  udata=data;
  for(i=0;i<len;i++)
    udata[i]= 
      ( (udata[i]>>24)&0xff ) |
      ( (udata[i]>>8)&0xff00 ) |
      ( (udata[i]<<8)&0xff0000 ) |
      ( (udata[i]<<24)&0xff000000 );
  return 1;
}

static int simple_swap_16(void *data,size_t len) {
  size_t i;
  uint16_t *udata;
  if( ((size_t)data)&0x1 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 16-bit integer is not 16-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  udata=data;
  for(i=0;i<len;i++)
    udata[i]= 
      ( (udata[i]>>8)&0xff ) |
      ( (udata[i]<<8)&0xff00 );
  return 1;
}

/**********************************************************************/
/* Use the GNU macros, which are specialized byteswap ASM instructions*/
/**********************************************************************/

static int macro_swap_64(void *data,size_t len) {
  size_t i;
  uint64_t *udata;
  if( ((size_t)data)&0x5 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 64-bit integer is not 64-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  udata=data;
  for(i=0;i<len;i++)
    udata[i]=bswap_64(udata[i]);
  return 1;
}

static int macro_swap_32(void *data,size_t len) {
  size_t i;
  uint32_t *udata;
  if( ((size_t)data)&0x3 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 32-bit integer is not 32-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  udata=data;
  for(i=0;i<len;i++)
    udata[i]=bswap_32(udata[i]);
  return 1;
}

static int macro_swap_16(void *data,size_t len) {
  size_t i;
  uint16_t *udata;
  if( ((size_t)data)&0x1 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 16-bit integer is not 16-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  udata=data;
  for(i=0;i<len;i++)
    udata[i]=bswap_16(udata[i]);
  return 1;
}

/**********************************************************************/
/* Use the GNU macros and do 1MB blocks at a time.  Control the block */
/* size through the BLOCK_COUNT_* macros (top of file)                */
/**********************************************************************/

static int block_macro_swap_32(void *data,size_t len) {
  size_t i,stop,j;
  uint32_t *udata;
  if( ((size_t)data)&0x3 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 32-bit integer is not 32-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  /* Swap full blocks first: */
  udata=data;
  stop=len/BLOCK_COUNT_32*BLOCK_COUNT_32;
  for(i=0;i<stop;i+=BLOCK_COUNT_32)
    for(j=0;j<BLOCK_COUNT_32;j++)
      udata[i+j]=bswap_32(udata[i+j]);
  /* Swap remainder */
  for(i=stop;i<len;i++)
    udata[i]=bswap_32(udata[i]);
  return 1;
}

static int block_macro_swap_16(void *data,size_t len) {
  size_t i,stop,j;
  uint16_t *udata;
  if( ((size_t)data)&0x1 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 16-bit integer is not 16-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  /* Swap full blocks first: */
  udata=data;
  stop=len/BLOCK_COUNT_16*BLOCK_COUNT_16;
  for(i=0;i<stop;i+=BLOCK_COUNT_16)
    for(j=0;j<BLOCK_COUNT_16;j++)
      udata[i+j]=bswap_16(udata[i+j]);
  /* Swap remainder */
  for(i=stop;i<len;i++)
    udata[i]=bswap_16(udata[i]);
  return 1;
}

static int block_macro_swap_64(void *data,size_t len) {
  uint64_t *udata;
  size_t i,stop,j;
  if( ((size_t)data)&0x5 != 0 ) {
    if (send_errors)
      fprintf(stderr,"ERROR: pointer to 64-bit integer is not 64-bit aligned (pointer is 0x%llx)\n",(long long)data);
    return 0;
  }
  /* Swap full blocks first: */
  udata=data;
  stop=len/BLOCK_COUNT_64*BLOCK_COUNT_64;
  for(i=0;i<stop;i+=BLOCK_COUNT_64)
    for(j=0;j<BLOCK_COUNT_64;j++)
      udata[i+j]=bswap_64(udata[i+j]);
  /* Swap remainder */
  for(i=stop;i<len;i++)
    udata[i]=bswap_64(udata[i]);
  return 1;
}



int fast_byteswap(void *data,int bytes,size_t count) {
  switch(bytes) {
  case 1: return 1;
  case 2: return simple_swap_16(data,count);
  case 4: return simple_swap_32(data,count);
  case 8: return macro_swap_64(data,count);
  default: return 0;
  }
}
