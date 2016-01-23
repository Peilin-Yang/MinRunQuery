/*                                                               */
/* Copyright 1984,1985,1986,1988,1989,1990,2003,2004,2005,2006,  */
/*   2007 by Howard Turtle                                       */

#define boolean int
#define true 1
#define false 0

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <ctype.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include "keyerr.h"
#include "keydef.h"
#include "keyfile.h"
/* platform specific file io bits */
#include "fileio.h"

#define log_errors true
#define fn_log_f printf

#ifdef log_buffers
FILE
*buffer_log;
#endif

struct shuffle_candidate {
  int
    lt_move_cnt,
    rt_move_cnt;
  unsigned
    lt_lc,
    lt_prefix_lc,
    mid_lc,
    mid_prefix_lc,
    rt_lc,
    rt_prefix_lc;
};

static int
  read_cnt=0,
  write_cnt=0,
  shuffle_cnt,
  shuffle_lt_zero_cnt=0,
  shuffle_rt_zero_cnt=0;

static level0_pntr
  null0_ptr = {max_segment,0,0},
  dummy_ptr = {0,0,0};
static struct leveln_pntr
  nulln_ptr = {max_segment,0};
static int
  power_of_two[20];

/*static int allocate_block();*/
static boolean allocate_rec();
static void replace_max_key();
static void deallocate_rec();
static void check_ix_block_compression();
static void update_index();
static void index_delete();
static void split_block();
static FILE *file_index();


/* uncompress_key_lc uncompresses the lc entry in a key in an   */
/*   index block.  Since the lc is much less that 2**14 we know */
/*   that the compressed lc must be in one or two bytes.        */

static int uncompress_key_lc(UINT16 *key_lc, unsigned char p[])
{
  if ( p[0]<128 ) {
    *key_lc = p[0];
    return(1);
  }
  else {
    *key_lc = (p[0] & 127) * 128 + p[1];
    return(2);
  }
}

/* uncompress_UINT16 uncompresses an integer compressed in      */
/*    array p and returns the number of bytes consumed to       */
/*    decompress the int.                                       */


static int uncompress_UINT16(UINT16 *i, unsigned char p[])
{int j=0; boolean done=false;

  if ( p[0]<128 ) {
    *i = p[0];
    return(1);
  }
  else {
    *i = 0;
    do {
      *i = *i | (p[j] & 127);
      if ( (p[j] & 128)!=0 ) {
        *i = *i << 7;
        j++;
      }
      else done = true;
    } while ( !done );
    return(j+1);
  }
}

/* uncompress_UINT32 uncompresses an integer compressed in      */
/*    array p and returns the number of bytes consumed to       */
/*    decompress the int.                                       */

static int uncompress_UINT32(UINT32 *i, unsigned char p[])
{int j=0; boolean done=false;

  *i = 0;
  do {
    *i = *i | (p[j] & 127);
    if ( (p[j] & 128)!=0 ) {
      *i = *i << 7;
      j++;
    }
    else done = true;
  } while ( !done );
  return(j+1);
}

/* uncompress_UINT64 uncompresses an integer compressed in      */
/*    array p and returns the number of bytes consumed to       */
/*    decompress the int.                                       */

static int uncompress_UINT64(UINT64 *i, unsigned char p[])
{int j=0; boolean done=false;

  *i = 0;
  do {
    *i = *i | (p[j] & 127);
    if ( (p[j] & 128)!=0 ) {
      *i = *i << 7;
      j++;
    }
    else done = true;
  } while ( !done );
  return(j+1);
}

/* Error handling.  All errors that require logging are       */
/*   signalled using one of the set_error calls.  These, in   */
/*   turn, call set_err which sets the fcb error code, sets   */
/*   file_ok to false if the error is considered fatal, and   */
/*   logs the error using fn_log_f.  "Normal" errors          */
/*   (xx_nokey, ateof, atbof, badkey, longrec, longkey) are   */
/*   not logged but are simply returned in f->error_code and  */
/*   will be reset on the next call.                          */
/* If log_errors==true then f->log file is opened (if         */
/*   necessary) and errors are logged to that file as well.   */
/*   Additional information may be written to the log file by */
/*   the caller (if log_errors is true).  Note that the same  */
/*   log_file is used for capturing trace information.        */

static boolean error_is_fatal(UINT32 error_code)
{boolean fatal=false;

  switch ( error_code ) {
    case  0: break;
    case  1: /* badopen_err */      fatal = true; break;
    case  2: /* badcreate_err */    fatal = true; break;
    case  3: /* smallfcb_err */     fatal = true; break;
    case  4: /* dltnokey_err */                 break;
    case  5: /* getnokey_err */                 break;
    case  6: /* notkeyfil_err */    fatal = true; break;
    case  7: /* filenotok_err */    fatal = true; break;
    case  8: /* badkey_err */                   break;
    case  9: /* maxlevel_err */     fatal = true; break;
    case 10: /* ateof_err */                    break;
    case 11: /* atbof_err */                    break;
    case 12: /* longrec_err */                  break;
    case 13: /* longkey_err */                  break;
    case 14: /* version_err */      fatal = true; break;
    case 15: /* seek_err */         fatal = true; break;
    case 16: /* read_err */         fatal = true; break;
    case 17: /* write_err */        fatal = true; break;
    case 18: /* segment_open_err */ fatal = true; break;
    case 19: /* notused */          fatal = true; break;
    case 20: /* bad_name_err */     fatal = true; break;
    case 21: /* bad_dlt_err */      fatal = true; break;
    case 22: /* max_key_err */      fatal = true; break;
    case 23: /* nospace_err */      fatal = true; break;
    case 24: /* free_insrt_err */   fatal = true; break;
    case 25: /* free_dlt_err */     fatal = true; break;
    case 26: /* alloc_rec_err */    fatal = true; break;
    case 27: /* dealloc_rec_err */  fatal = true; break;
    case 28: /* alloc_buf_err */    fatal = true; break;
    case 29: /* move_rec_err */     fatal = true; break;
    case 30: /* bad_close_err */    fatal = true; break;
    case 31: /* ix_struct_err */    fatal = true; break;
    case 32: /* read_only_err */                  break;
    case 33: /* repl_max_key_err */ fatal = true; break;
    case 34: /* data_lc_err */                    break;
    case 35: /* insert_err */       fatal = true; break;
    case 36: /* ix_compress_err */                break;
    case 37: /* not_supported_err */              break;
    case 38: /* move_keys_err */    fatal = true; break;
    default: /* illegal_err code */ fatal = true; break;
  }
  return(fatal);
}

static void set_err(struct fcb *f, UINT32 err)
{
   f->error_code = err;
   if ( error_is_fatal(err) ) f->file_ok = false;
   if ( log_errors && f->log_file==NULL ) {
     f->log_file = fopen("kf_error_log","wb");
   }
}

static void set_error(struct fcb *f, int err, char caption[])
{
  set_err(f,(UINT32)err);
  fn_log_f("%s\n",caption);
  if ( log_errors ) fprintf(f->log_file,"%s\n",caption);
}

static void set_error1(struct fcb *f, int err, char caption[], int code)
{
  set_err(f,(UINT32)err);
  fn_log_f("%s%d\n",caption,code);
  if ( log_errors ) fprintf(f->log_file,"%s%d\n",caption,code);
}

static void set_error2(struct fcb *f, int err, char caption[], int code1, int code2)
{
  set_err(f,(UINT32)err);
  fn_log_f("%s%d/%d\n",caption,code1,code2);
  if ( log_errors ) fprintf(f->log_file,"%s%d/%d\n",caption,code1,code2);
}

/* Error checking.  Three fields in the fcb are used for error */
/*   management.  f->marker is set to keyf when the file is    */
/*   created and is never changed.  Any fcb with a different   */
/*   value is not useable.  f->file_ok is set true when the    */
/*   file is created and is turned off if an error occurs that */
/*   is so serious that the file is probably damaged (call to  */
/*   error_is_fatal).  f->error_code is set for any error      */
/*   condition.  Some errors are considered transient (e.g.,   */
/*   ateof, atbof, xxnokey,...) and are reset on the next call */
/*   to the package.  All others are considered permanent and  */
/*   are not reset.                                            */

boolean check_fcb(struct fcb *f)
{ boolean ok;

  ok = (f->marker==keyf) && f->file_ok && !error_is_fatal(f->error_code);
  if ( ok ) f->error_code = no_err;
  return(ok);
}

static boolean set_up(struct fcb *f, unsigned char key[], unsigned key_lc, struct key *k)
{
  if ( !check_fcb(f) ) return(false);
  k->lc = key_lc;
  if ( k->lc>0 && k->lc<maxkey_lc ) {
    memcpy(k->text,key,(size_t)key_lc); return(true);
  }
  else {
    f->error_code = badkey_err; k->lc = 0;
    return(false);
  }
}

/* Pointer manipulation */

static boolean gt_n_pntr(struct leveln_pntr p1, struct leveln_pntr p2)
{
  if ( p1.segment<p2.segment ) return(false);
  else if ( p1.segment>p2.segment ) return(true);
  else return( p1.block>p2.block );
}

static int pntr_sc(struct ix_block *b, int ix)
{int sc,lc; UINT16 key_lc;

  sc = b->keys[ix];
  lc = uncompress_key_lc(&key_lc,(unsigned char *)b->keys+sc);
  return(sc+lc+key_lc);
}

/* unpack0_ptr_and_rec unpacks the level0_pntr for buf[ix] and extracts the   */
/*   data into rec (up to max_rec_lc bytes).  The number of bytes decoded to  */
/*   get the pointer is returned as the function value.  The number of data   */
/*   bytes in rec is returned in rec_lc.                                      */
/* If max_rec_lc==0 then no data is returned.  If the data rec is on disk     */
/*   it is read and p will have the normal on-disk segment/lc/sc values.  If  */
/*   the data rec is in the index block it is copied directly, segment is set */
/*   to max_segment and sc is set to zero.                                   */
/* If the caller wants to unpack the pointer but not get the rec then rec     */
/*   should be pointed to the data_rec field in p and max_rec_lc set to       */
/*   f->data_in_index_lc                                                      */

static int unpack0_ptr_and_rec(struct fcb *f, buffer_t *buf, int ix, level0_pntr *p,
  unsigned char rec[], unsigned *rec_lc, unsigned max_rec_lc)
{int lc; UINT64 esc; unsigned char *cp; size_t size=0; FILE *file;

  cp = (unsigned char *)(buf->b.keys) + pntr_sc(&(buf->b),ix);
  lc = uncompress_UINT32(&(p->lc),cp);
  *rec_lc = p->lc;
  if ( (unsigned)*rec_lc>max_rec_lc ) *rec_lc = max_rec_lc;
  if ( p->lc > f->data_in_index_lc ) {
    lc = lc + uncompress_UINT64(&esc,cp+lc);
    p->sc = esc >> 1;
    p->sc = p->sc * rec_allocation_unit;
    if ( (esc & 1)>0 ) lc = lc + uncompress_UINT16(&(p->segment),cp+lc);
    else p->segment = 0;

    file = file_index(f,p->segment);
    if ( fseeko(file,(FILE_OFFSET)p->sc,0)!=0 ) f->error_code = seek_err;
    else size = fread(rec,(size_t) 1,(size_t) *rec_lc,file);
    if ( size!=(size_t)*rec_lc ) f->error_code = read_err;
  }
  else {
    p->segment = max_segment;
    p->sc = 0;
    memcpy(rec,cp+lc,(size_t)*rec_lc);
    lc = lc + p->lc;
  }
  return(lc);
}

/* unpackn_pntr unpacks the ix_th leveln_pntr in block b   */
/*   into p and returns the number of characters occupied  */
/*   by the compressed pointer.                            */

int unpackn_ptr(struct ix_block *b, int ix, struct leveln_pntr *p)
{int lc; unsigned char *cp; UINT64 block;

  cp = (unsigned char *) b->keys + pntr_sc(b,ix);

  lc = uncompress_UINT64(&block,cp);
  p->block = block >> 1;
  if ( (block & 1)>0 ) lc = lc + uncompress_UINT16(&(p->segment),cp+lc);
  else p->segment = 0;

  return( lc );
}

/* get_nth_key returns the nth key from block b.  The length of */
/*   the compressed lc field is returned as the fn value.       */

int get_nth_key(struct ix_block *b, struct key *k, int n)
{int lc=0; UINT16 key_lc; unsigned char *entry_sc;

  if ( n<0 || n>=b->keys_in_block ) k->lc = 0;
  else {
    mvc(b->keys,keyspace_lc-b->prefix_lc,k->text,0,b->prefix_lc);
    entry_sc = (unsigned char *) b->keys + b->keys[n];
    lc = uncompress_key_lc(&key_lc,entry_sc);
    k->lc = key_lc + b->prefix_lc;
    mvc(entry_sc,lc,k->text,b->prefix_lc,key_lc);
  }
  return(lc);
}

/**** I/O ***/


/* init_file_name separates the file name and any extension */
/*   and saves the two parts in the fcb                     */

static void init_file_name(struct fcb *f, char id[])
{int i; unsigned name_lc, f_lc, ext_lc = 0;

  name_lc = (unsigned) strlen(id);
  if (name_lc > max_filename_lc + max_extension_lc)
    set_error(f,bad_name_err,"file name too long");
  i = name_lc - 1;
  /* scan  from right to left
     stop when we hit either a . or a path separator.
  */
  while ( i>=0 && id[i]!='.' && id[i]!=PATH_SEPARATOR) {
    i--;
    ext_lc++;
  }
  if (i >= 0 && id[i] == '.') {
    f_lc = i;
    ext_lc++;
  }
  else {
    f_lc = name_lc;
    ext_lc = 0;
  }
  if (f_lc>=max_filename_lc) set_error(f,bad_name_err,"file name too long");
  else {
    strncpy(f->file_name, id, (size_t)f_lc);
    f->file_name[f_lc] = '\0';
  }
  if ( ext_lc>=max_extension_lc ) set_error(f,bad_name_err,"file extension too long");
  else {
    strncpy(f->file_extension, id + i, (size_t)ext_lc);
    f->file_extension[ext_lc] = '\0';
  }
}

/* build_segment_name builds a segment name by appending the segment */
/*   number to the file name and then appending any extension.       */

static void build_segment_name(struct fcb *f, unsigned segment, char name[])
{int suffix_lc; size_t name_lc;

  strcpy(name,f->file_name);
  if (segment>0) {
    name_lc = strlen(name);
    suffix_lc = sprintf(name+name_lc,"$%d",segment);
    name[name_lc+suffix_lc] = '\0';
  }
  strcat(name,f->file_extension);
}

static void byte_swap_UINT16s(unsigned char s[], int cnt)
{unsigned int i=0; unsigned char ch;

  while ( i<cnt*sizeof(UINT16) ) {
    ch = s[i];
    s[i] = s[i+1];
    s[i+1] = ch;
    i = i + sizeof(UINT16);
  }
}

static void byte_swap_UINT32(unsigned char n[])
{unsigned char ch;

  ch = n[0];
  n[0] = n[3];
  n[3] = ch;
  ch = n[1];
  n[1] = n[2];
  n[2] = ch;
}

static void byte_swap_UINT64(unsigned char n[])
{unsigned char ch;

  ch = n[0];
  n[0] = n[7];
  n[7] = ch;
  ch = n[1];
  n[1] = n[6];
  n[6] = ch;
  ch = n[2];
  n[2] = n[5];
  n[5] = ch;
  ch = n[3];
  n[3] = n[4];
  n[4] = ch;
}

static unsigned char read_byte(struct fcb *f, FILE *file)
{unsigned char ch=0;

  if ( fread(&ch,sizeof(char),(size_t)1,file)!=1 )
    set_error(f,read_err,"read_byte failed");
  return(ch);
}


static UINT16 read_UINT16(struct fcb *f, FILE *file)
{UINT16 n; unsigned char ch;
 unsigned char *p = (unsigned char *)&n;

  if ( fread(&n,sizeof(UINT16),(size_t)1,file)!=1 ) {
    set_error(f,read_err,"read_UINT16 failed");
    return(0);
  }
  if ( f->byte_swapping_required ) {
    ch = p[1];
    p[1] = p[0];
    p[0] = ch;
  }
  return(n);
}


static UINT32 read_UINT32(struct fcb *f, FILE *file)
{UINT32 n;

  if ( fread(&n,sizeof(UINT32),(size_t)1,file)!=1 ) {
    set_error(f,read_err,"read_UINT32 failed");
    return(0);
  }
  if ( f->byte_swapping_required ) byte_swap_UINT32((unsigned char *) &n);
  return(n);
}

static UINT64 read_UINT64(struct fcb *f, FILE *file)
{UINT64 n;

  if ( fread(&n,sizeof(UINT64),(size_t)1,file)!=1 ) {
    set_error(f,read_err,"read_UINT64 failed");
    return(0);
  }
  if ( f->byte_swapping_required ) byte_swap_UINT64((unsigned char *) &n);
  return(n);
}

static boolean read_fib(struct fcb *f,char id[], boolean byte_swapping_required,
  boolean read_only)
{int i,j; FILE_OFFSET position; FILE *file;

  file = fopen(id,"rb");
  if ( file==NULL ) set_error(f,badopen_err,"Couldn't open fib");
  else if ( fseeko(file,(FILE_OFFSET) 0,0)!=0 ) set_error(f,badopen_err,"fib seek failed");
  else {
    f->byte_swapping_required = byte_swapping_required;
    f->read_only = read_only;

    f->error_code = read_UINT32(f,file);
    f->version = read_UINT32(f,file);
    f->sub_version = read_UINT32(f,file);
    f->segment_cnt = read_UINT32(f,file);
    for ( i=0; i<max_index; i++) f->primary_level[i] = read_UINT32(f,file);
    f->marker = read_UINT32(f,file);
    f->file_ok = read_UINT32(f,file);
    for (i=0; i<max_level; i++)
      for (j=0; j<max_index; j++) {
        f->first_free_block[i][j].segment = read_UINT16(f,file);
        f->first_free_block[i][j].block = read_UINT64(f,file);
      }
    for (i=0; i<max_level; i++)
      for (j=0; j<max_index; j++) {
        f->first_at_level[i][j].segment = read_UINT16(f,file);
        f->first_at_level[i][j].block = read_UINT64(f,file);
      }
    for (i=0; i<max_level; i++)
      for (j=0; j<max_index; j++) {
        f->last_pntr[i][j].segment = read_UINT16(f,file);
        f->last_pntr[i][j].block = read_UINT64(f,file);
      }
    f->max_file_lc = read_UINT64(f,file);
    for (i=0; i<max_segment; i++) f->segment_length[i] = read_UINT64(f,file);
    f->data_in_index_lc = read_UINT32(f,file);
    position = ftello(file);
    if ( position!=fib_lc_on_disk ) set_error1(f,badopen_err,"Read fib failed, position=",(int)position);
    fclose(file);
  }
  return(f->error_code==no_err);
}

static void read_page(struct fcb *f, struct leveln_pntr p, block_type_t *buf)
{FILE *file; FILE_OFFSET offset;

  read_cnt++;
  file = file_index(f,p.segment);
  offset = (p.block) << f->block_shift;
  if ( file==NULL ) set_error(f,read_err,"Bad file in read_page");
  else if ( fseeko(file,offset,0)!=0 )
    set_error(f,seek_err,"Seek failed in read_page");
  else {
    buf->keys_in_block = read_UINT16(f,file);
    buf->chars_in_use = read_UINT16(f,file);
    buf->index_type = read_byte(f,file);
    buf->prefix_lc = read_byte(f,file);
    buf->unused = read_byte(f,file);
    buf->level = read_byte(f,file);
    buf->next.segment = read_UINT16(f,file);
    buf->next.block = read_UINT64(f,file);
    buf->prev.segment = read_UINT16(f,file);
    buf->prev.block = read_UINT64(f,file);
    fread(buf->keys,(size_t) 1, (size_t) keyspace_lc,file);
    if ( ftello(file)!=(FILE_OFFSET)(offset+block_lc) )
      set_error1(f,read_err,"I/O failure in read_page, bytes read=",(int)(ftello(file)-offset));
    if ( f->byte_swapping_required )
      byte_swap_UINT16s((unsigned char *)buf->keys,buf->keys_in_block);
  }
}

/* vacate_file_index finds the LRU file_index, closes the segment */
/*   currently in use, marks the segment as closed and returns an */
/*   index into open_file to be used for segment I/O              */

static int vacate_file_index(struct fcb *f)
{int i,oldest,age,max_age;

  oldest = 0; max_age = 0;
  for ( i=0; i<f->open_file_cnt; i++ ) {
    age = f->current_age - f->file_age[i];
    if ( age>max_age ) {
      oldest = i; max_age = age;
    }
  }
  f->segment_ix[f->open_segment[oldest]] = max_files;
  fclose(f->open_file[oldest]);
  return(oldest);
}

/* open_segment opens a file segment.  If it is new it is opened in */
/*   write+ mode otherwise it is opened in read+ mode.  If the open */
/*   fails then f->open_file[ix] is set to NULL and f->error_code is*/
/*   set.  In any case the directories segment_ix[] and             */
/*   open_segment[] are set.  */

static void open_segment(struct fcb *f, unsigned segment, int ix)
{char name[max_filename_lc+10]; char* mode;

  build_segment_name(f,segment,name);
  if ( segment >= (unsigned)f->segment_cnt ) {
    if ( f->read_only ) {
      set_error(f,read_only_err,"Read only_err");
      return;
    }
    else f->open_file[ix] = fopen(name,"wb+");
  }
  else {
    mode = f->read_only ? "rb" : "rb+";
    f->open_file[ix] = fopen(name,mode);
  }
  if (f->open_file[ix]==NULL) set_error(f,segment_open_err,"Bad file in open_segment");
  f->segment_ix[segment] = ix;
  f->open_segment[ix] = segment;
}

static int file_ix(struct fcb *f, unsigned segment)
{int ix;

  ix = f->segment_ix[segment];
  if (ix<max_files) /* have a file open */;
  else if (f->open_file_cnt<max_files) {
    ix = f->open_file_cnt;
    f->open_file_cnt++;
    open_segment(f,segment,ix);
  }
  else {
    ix = vacate_file_index(f);
    open_segment(f,segment,ix);
  }
  f->file_age[ix] = f->current_age;
  return(ix);
}


/* file_index returns a file index to be used for I/O to a given   */
/*   segment of the keyed file.  If there is a file index open for */
/*   the segment, it is returned.  If there is an unused file   */
/*   index it is opened for the segment and returned.  Otherwise   */
/*   the LRU index is closed and reopened for the segment.         */

static FILE *file_index(struct fcb *f, unsigned segment)
{int ix;

  if ( segment<max_segment ) {
    ix = file_ix(f,segment);
    return(f->open_file[ix]);
  }
  else return(NULL);
}

static void set_position(struct fcb *f, int index, struct leveln_pntr b, int ix)
{
  f->position[index] = b; f->position_ix[index] = ix;
}


/* Buffer handling */

/* Buffers are managed using two orthogonal structures.  The    */
/*   first is a conventional hash table that has been allocated */
/*   at the end of the fcb and whose base address is in         */
/*   buf->buffer_pool.buf_hash_table.  This hash table contains the index   */
/*   of the first buffer containing a block with the            */
/*   corresponding hash_value; an empty hash table entry        */
/*   contains -1.  If there are multiple buffers containing     */
/*   blocks with the same hash value then they are chained      */
/*   using the hash_next field in each buffer.  The hash table  */
/*   is searched using search_hash_chain() and managed using    */
/*   hash_chain_insert() and hash_chain_remove().               */
/* The second structure is a pair of linked lists that list     */
/*   buffers from youngest to oldest.  One list is maintained   */
/*   for buffers containing level_0 blocks, the other is for    */
/*   level_n blocks.  youngest_buffer[x] (x=level_0 or level_n) */
/*   is an index to the buffer containing the MRU block,        */
/*   oldest_buffer[x] contains the index of the LRU block.      */
/*   buffer_in_use[x] is the number of buffers in each chain.   */

/* reset_ages is called when f->current_age reaches INT_MAX.    */
/*   The age of all open files is set to 0 and                  */
/*   f->current_age is set to 0.                                */

static void reset_ages(struct fcb *f)
{int i;

  for (i=0; i<f->open_file_cnt; i++) f->file_age[i] = 0;
  f->current_age = 0;
}


#define hash_value(b,limit)  ((((int)b.block) + b.segment) % limit)


static int search_hash_chain(struct fcb *f, struct leveln_pntr block)
{int k,next,bufix=-1/*,cnt=0*/;

  
  k =  hash_value(block,f->buffer_pool.buf_hash_entries);
  next = f->buffer_pool.buf_hash_table[k];
  while ( next>=0 ) {
    if ( eqn_pntr(f->buffer_pool.buffer[next].contents,block) ) {
      bufix = next; next = -1;
    }
    else next = f->buffer_pool.buffer[next].hash_next;
  }
  /*  fprintf(f->log_file,"  searched hash chain %d for %d/%d, ",k,block.segment,block.block);
  if ( bufix<0 ) fprintf(f->log_file,"not found, cnt=%d\n",cnt);
  else fprintf(f->log_file,"found in bufix=%d, cnt=%d\n",bufix,cnt);*/
  return(bufix);
}

/* hash_chain_insrt inserts a buffer in a hash_chain     */
/*   The block pointer is hashed and the hash table is   */
/*   checked.  If the hash table entry<0 then the        */
/*   chain is empty and the buffer inserted as a         */
/*   chain of length one.  If the entry>=0 then the      */
/*   chain is searched and the buffer inserted.          */
/* It assumes that the the buffer contents and           */
/*   hash_next fields have been set prior to call.       */

static void hash_chain_insert(struct fcb *f, int bufix)
{int k,next,last=-1; struct leveln_pntr block;

  block = f->buffer_pool.buffer[bufix].contents;
  k = hash_value(block,f->buffer_pool.buf_hash_entries);
  next = f->buffer_pool.buf_hash_table[k];
  if ( next<0 ) {
    f->buffer_pool.buf_hash_table[k] = bufix;
    f->buffer_pool.buffer[bufix].hash_next = -1;
  }
  else {
    while ( next>=0 && gt_n_pntr(block,f->buffer_pool.buffer[next].contents) ) {
      last = next; next = f->buffer_pool.buffer[next].hash_next;
    }
    if ( last<0 ) {
      f->buffer_pool.buffer[bufix].hash_next = f->buffer_pool.buf_hash_table[k];
      f->buffer_pool.buf_hash_table[k] = bufix;
    }
    else {
      f->buffer_pool.buffer[last].hash_next = bufix;
      f->buffer_pool.buffer[bufix].hash_next = next;
    }
  }
}

/* hash_chain_remove removes buffer[bufix] from its hash chain */
/* It assumes that the the buffer contents and           */
/*   hash_next fields have been set prior to call.       */

static void hash_chain_remove(struct fcb *f, int bufix)
{int k,next,last=0; struct leveln_pntr block;

  block = f->buffer_pool.buffer[bufix].contents;
  k = hash_value(block,f->buffer_pool.buf_hash_entries);
  next = f->buffer_pool.buf_hash_table[k];
  if ( next==bufix ) f->buffer_pool.buf_hash_table[k] = f->buffer_pool.buffer[bufix].hash_next;
  else {
    while ( (next>=0) && !eqn_pntr(block,f->buffer_pool.buffer[next].contents) ) {
      last = next; next = f->buffer_pool.buffer[next].hash_next;
    }
    if ( next<0 ) {
      set_error1(f,alloc_buf_err,"Tried to remove nonexistent buffer, bufix=",bufix);
    }
    else f->buffer_pool.buffer[last].hash_next = f->buffer_pool.buffer[next].hash_next;
  }
  f->buffer_pool.buffer[bufix].hash_next = -1;
}

/* make_buffer_youngest removes the buffer in bufix from the */
/*   age chain and inserts it as the youngest buffer         */
/* It assumes that the buffer older and younger fields are   */
/*   set prior to call.                                      */

static void make_buffer_youngest(struct fcb *f,int bufix)
{int older,younger;

  older = f->buffer_pool.buffer[bufix].older;
  younger = f->buffer_pool.buffer[bufix].younger;
  if ( younger>=0 ) { /* not allready youngest */
    if ( older==-1 ) {
      f->oldest_buffer = younger;
      f->buffer_pool.buffer[younger].older = -1;
    }
    else {
      f->buffer_pool.buffer[older].younger = younger;
      f->buffer_pool.buffer[younger].older = older;
    }
    f->buffer_pool.buffer[f->youngest_buffer].younger = bufix;
    f->buffer_pool.buffer[bufix].younger = -1;
    f->buffer_pool.buffer[bufix].older = f->youngest_buffer;
    f->youngest_buffer = bufix;
  }
}


static void init_buffer_hash_fields(struct fcb *f, int i, struct leveln_pntr *b)
{
  f->buffer_pool.buffer[i].older = -1;
  f->buffer_pool.buffer[i].younger = -1;
  f->buffer_pool.buffer[i].hash_next = -1;
}

/* initialize_buffer initializes all of the buffer header fields except the */
/*   hashing fields (older, younger, hash_next).  It may reinitialize some  */
/*   fields (e.g., contents) that were set by the hashing functions.        */

static void initialize_buffer(struct fcb *f, int bufix, struct leveln_pntr *contents)
{
  f->buffer_pool.buffer[bufix].contents = *contents;
  f->buffer_pool.buffer[bufix].modified = false;
  f->buffer_pool.buffer[bufix].lock_cnt = 0;
  f->buffer_pool.buffer[bufix].search_cnt = 0;
}

#define buddy_window 32

static void write_UINT16(struct fcb *f, FILE *file, UINT16 *i)
{UINT16 n; unsigned char ch;
 unsigned char *p = (unsigned char *)&n;

  n = *i;
  if ( f->byte_swapping_required ) {
    ch = p[0];
    p[0] = p[1];
    p[1] = ch;
  }
  if ( fwrite(&n,sizeof(UINT16),(size_t)1,file)!=1 )
    set_error(f,write_err,"write failed in write_UINT16\n");
}

static void write_UINT16s(struct fcb *f, FILE *file, unsigned char s[], unsigned int cnt)
{unsigned int i; unsigned char swapped[keyspace_lc];

  if ( f->byte_swapping_required ) {
    i = 0;
    while ( i<cnt*sizeof(UINT16) ) {
      swapped[i] = s[i+1];
      swapped[i+1] = s[i];
      i = i + sizeof(UINT16);
    }
    if ( fwrite(swapped,sizeof(UINT16),(size_t)cnt,file)!=cnt )
    set_error(f,write_err,"write_UINT16s failed\n");
  }
  else {
    if ( fwrite(s,sizeof(UINT16),(size_t)cnt,file)!=cnt )
      set_error(f,write_err,"write_UINT16s failed\n");
  }
}

static void write_UINT32(struct fcb *f, FILE *file, UINT32 i)
{UINT32 n;

  n = i;
  if ( f->byte_swapping_required ) byte_swap_UINT32((unsigned char *) &n);
  if ( fwrite(&n,sizeof(UINT32),(size_t)1,file)!= 1 )
    set_error(f,write_err,"write failed in write_UINT32\n");
}

static void write_UINT64(struct fcb *f, FILE *file, UINT64 i)
{UINT64 n;

  n = i;
  if ( f->byte_swapping_required ) byte_swap_UINT64((unsigned char *) &n);
  if ( fwrite(&n,sizeof(UINT64),(size_t)1,file)!= 1 )
    set_error(f,write_err,"write failed in write_UINT64\n");
}

static void write_page(struct fcb *f, struct leveln_pntr p, block_type_t *buf)
{int pntr_lc,remaining; FILE *file; FILE_OFFSET offset;

  write_cnt++;
  if ( f->read_only ) {
    f->error_code = read_only_err;
    return;
  }
  file = file_index(f,p.segment);
  offset = (p.block) << f->block_shift;
  if ( file==NULL ) set_error(f,write_err,"Bad file in write_page");
  else if ( fseeko(file,offset,0)!=0 )
    set_error(f,seek_err,"Seek error in write_page");
  else {
    write_UINT16(f,file,&(buf->keys_in_block));
    write_UINT16(f,file,&(buf->chars_in_use));
    if ( fwrite(&(buf->index_type),sizeof(char),(size_t)1,file)!=1 )
      set_error(f,write_err,"write byte failed");
    if ( fwrite(&(buf->prefix_lc), sizeof(char),(size_t)1,file)!=1 )
      set_error(f,write_err,"write byte failed");
    if ( fwrite(&(buf->unused),sizeof(char),(size_t)1,file)!=1 )
      set_error(f,write_err,"write byte failed");
    if ( fwrite(&(buf->level),     sizeof(char),(size_t)1,file)!=1 )
      set_error(f,write_err,"write byte failed");
    write_UINT16(f,file,&(buf->next.segment));
    write_UINT64(f,file,buf->next.block);
    write_UINT16(f,file,&(buf->prev.segment));
    write_UINT64(f,file,buf->prev.block);
    write_UINT16s(f,file,(unsigned char *)buf->keys,(unsigned)buf->keys_in_block);
    pntr_lc = buf->keys_in_block * sizeof(UINT16);
    remaining = keyspace_lc - pntr_lc;
    fwrite((char *)buf->keys+pntr_lc,(size_t) 1, (size_t) remaining,file);
    if ( ftello(file)!=(FILE_OFFSET)(offset+block_lc) )
      set_error1(f,read_err,"I/O failure in write_page, bytes written=",(int)(ftello(file)-offset));
#ifdef VERIFY_WRITES
    block_type_t temp;
    read_page(f,p,&temp);
    if ( !eq_block(buf,&temp) ) set_error(f,write_err,"**write_page failed, doesn't match orig\n");
#endif
  }
}

static int write_block_and_buddies(struct fcb *f, int bufix)
{
  int i,younger_buddies=0,older_buddies=0,ix,buddy_list[buddy_window];
  struct leveln_pntr buddy; boolean done=false;

  buddy = f->buffer_pool.buffer[bufix].contents;
  while ( buddy.block>0 && younger_buddies<buddy_window && !done) {
    buddy.block--;
    ix = search_hash_chain(f,buddy);
    if ( ix>=0 && f->buffer_pool.buffer[ix].modified && f->buffer_pool.buffer[ix].lock_cnt==0) {
      buddy_list[younger_buddies] = ix;
      younger_buddies++;
    }
    else done = true;
  }
  for (i=younger_buddies-1; i>=0; i--) {
    ix = buddy_list[i];
    write_page(f,f->buffer_pool.buffer[ix].contents,&(f->buffer_pool.buffer[ix].b));
    f->buffer_pool.buffer[ix].modified = false;
  }

  write_page(f,f->buffer_pool.buffer[bufix].contents,&(f->buffer_pool.buffer[bufix].b));

  buddy = f->buffer_pool.buffer[bufix].contents;
  done = false;
  while ( older_buddies<buddy_window && !done) {
    buddy.block++;
    ix = search_hash_chain(f,buddy);
    if ( ix>=0 && f->buffer_pool.buffer[ix].modified && f->buffer_pool.buffer[ix].lock_cnt==0) {
      buddy_list[older_buddies] = ix;
      older_buddies++;
    }
    else done = true;
  }
  for (i=0; i<older_buddies; i++) {
    ix = buddy_list[i];
    write_page(f,f->buffer_pool.buffer[ix].contents,&(f->buffer_pool.buffer[ix].b));
    f->buffer_pool.buffer[ix].modified = false;
  }
#ifdef log_buffers
  if ( younger_buddies>0 || older_buddies>0 ) {
    fprintf(buffer_log,"Wrote block %d/%lu, %d younger buddies, %d older buddies\n",
      f->buffer_pool.buffer[bufix].contents.segment,
      f->buffer_pool.buffer[bufix].contents.block,younger_buddies,older_buddies);
  }
#endif

  return(younger_buddies+older_buddies+1);
}

/* vacate_oldest_buffer is called when a new buffer is needed          */
/*   if there are unallocated buffers then the next one is             */
/*   added to the buffer chain and returned.  If all buffers           */
/*   are in use then the oldest unlocked buffer is flushed             */
/*   (if necessary) and returned                                       */
/* If an unallocated buffer is returned then it is initialized (by     */
/*   init_buffer_hash_fields) but further initialization by the caller */
/*   will be     */
/*   required.  If a buffer is vacated then only the buffer management */
/*   fields (older, younger, hash_next) are set.  The caller should    */
/*   not modify those fields but should initialize everything else as  */
/*   necessary.                                                        */

static int vacate_oldest_buffer(struct fcb *f, struct leveln_pntr *b)
{int oldest,cnt=0,locked_cnt=0,i; boolean done; struct leveln_pntr oldest_block;

  if ( f->buffer_pool.buffers_in_use < f->buffer_pool.buffers_allocated ) {
    oldest = f->buffer_pool.buffers_in_use;
    init_buffer_hash_fields(f,oldest,b);
    initialize_buffer(f,oldest,b);
    if ( f->buffer_pool.buffers_in_use==0 ) {
      f->youngest_buffer = oldest; f->oldest_buffer = oldest;
    }
    else {
      f->buffer_pool.buffer[f->youngest_buffer].younger = oldest;
      f->buffer_pool.buffer[oldest].older = f->youngest_buffer;
      f->youngest_buffer = oldest;
    }
    f->buffer_pool.buffers_in_use++;
#ifdef log_buffers
    fprintf(buffer_log,"Paging block %d/%lu into unused buffer %d, ",b->segment,b->block,oldest);
    fprintf(buffer_log,"MRU chain after insert:");
    print_buffer_MRU_chain(buffer_log,f);
#endif
  }
  else {
    do {
      oldest = f->oldest_buffer;
      make_buffer_youngest(f,oldest);
      cnt++;
      if ( cnt>f->buffer_pool.buffers_allocated ) {
        done = true; 
        set_error1(f,alloc_buf_err,"Couldn't allocate a buffer, allocated=",f->buffer_pool.buffers_allocated);
      }
      else if ( f->buffer_pool.buffer[oldest].lock_cnt>0 ) {
        done = false; locked_cnt++;
      }
      else done = true;
    }
    while ( !done );
    oldest_block = f->buffer_pool.buffer[oldest].contents;
    if ( f->buffer_pool.buffer[oldest].modified ) {
      write_block_and_buddies(f,oldest);
    }
    hash_chain_remove(f,oldest);
    initialize_buffer(f,oldest,b);
#ifdef log_buffers
    fprintf(buffer_log,"Paging block %d/%lu into buffer %d",b->segment,b->block,oldest);
    if ( f->buffer_pool.buffer[oldest].modified ) fprintf(buffer_log,", flushing ");
    else fprintf(buffer_log,", replacing ");
    fprintf(buffer_log,"block %d/%lu, ",oldest_block.segment,oldest_block.block);
    fprintf(buffer_log,"MRU chain after insert:");
    print_buffer_MRU_chain(buffer_log,f);
#endif
  }
  return(oldest);
}

static void set_empty_block_prefix(struct ix_block *b, struct key *prefix, unsigned prefix_lc)
{
  mvc(prefix->text,0,b->keys,keyspace_lc-prefix_lc,prefix_lc);
  b->chars_in_use = prefix_lc;
  b->prefix_lc = prefix_lc;
}

static void initialize_index_block(struct ix_block *b, int index, unsigned lvl,
  struct key *prefix, unsigned prefix_lc)
{
  set_empty_block_prefix(b,prefix,prefix_lc);
  b->keys_in_block = 0;
  b->index_type = index;
  b->level = lvl;
  b->next = nulln_ptr;
  b->prev = nulln_ptr;
}

static int get_index(struct fcb *f, struct leveln_pntr b)
{int bufix,index_type; struct key dummy;

  f->current_age++;
  if ( f->current_age==INT_MAX ) reset_ages(f);
  bufix = search_hash_chain(f,b);

  if ( bufix>=0 ) {
    make_buffer_youngest(f,bufix);
#ifdef log_buffers
    fprintf(buffer_log,"Found lvl %d block %d/%ld in buffer %d\n",f->buffer_pool.buffer[bufix].b.level,
      b.segment,b.block,bufix);
#endif
  }
  else {
    bufix = vacate_oldest_buffer(f,&b);
    hash_chain_insert(f,bufix);
    read_page(f,b,&(f->buffer_pool.buffer[bufix].b));
  }
  if ( f->error_code==no_err ) {
    index_type = f->buffer_pool.buffer[bufix].b.index_type;
    f->mru_at_level[f->buffer_pool.buffer[bufix].b.level][index_type] = b;
  }
  else initialize_index_block(&(f->buffer_pool.buffer[bufix].b),user_ix,level_zero,&dummy,(unsigned)0);
  return(bufix);
}

/* Index searching */

/* Interior index blocks contain entries that point to the subtree
 *   containing keys <= entry key (and greater than the previous
 *   entry key).  Each level in the index has an additional pointer
 *   that points to the last subtree -- the subtree containing keys
 *   <= the largest possible key.  This last pointer is not stored
 *   in the index block but is found in last_pntr[].
 */

/* compare_key compares a key k with the ix^th entry stored in an   */
/*   index block.                                                   */

static int compare_key(unsigned char k[], UINT32 klc, struct ix_block *b, int ix)
{int r,lc; UINT16 key_lc; unsigned char *entry_ptr;

  entry_ptr = (unsigned char *)b->keys + b->keys[ix];
  lc = uncompress_key_lc(&key_lc,entry_ptr);

  if ( klc<=key_lc ) {
    r = memcmp(k,entry_ptr+lc,(size_t)klc );
    if (r<0) return(cmp_less);
    else if (r>0) return(cmp_greater);
    else if ( klc==key_lc ) return(cmp_equal);
    else return(cmp_less);
  }
  else {
    r = memcmp(k,entry_ptr+lc,(size_t)key_lc );
    if (r<0) return(cmp_less);
    else if (r>0) return(cmp_greater);
    else if ( klc==key_lc ) return(cmp_equal);
    else return(cmp_greater);
  }
}

/* search_block searches the block for the first entry>=k             */
/*   if k = some entry then  found=true   and ix is entry             */
/*   if k < some entry then  found=false  and ix is entry             */
/*   if k > all entries then found=false ix = keys_in_block           */
/* If the block is empty (happens with freespace blocks where the     */
/*   only block is the primary) then the key is treated as greater    */
/*   than all entries.                                                */

static int search_block(struct fcb *f, int bufix, struct key *k, boolean *found)
{int mid,high,ix=0,r=0,prefix_lc,klc;
 unsigned char *t; struct ix_block *b;

  *found = false;
  b = &(f->buffer_pool.buffer[bufix].b);
  if ( b->keys_in_block>0 ) {
    /*    f->buffer_pool.buffer[bufix].search_cnt++;*/
    prefix_lc = b->prefix_lc;
    if ( k->lc<prefix_lc ) {
      r = memcmp(k->text,(char *) b->keys+keyspace_lc-prefix_lc,(size_t)k->lc );
      if (r>0) ix = b->keys_in_block;
      else ix = 0;
    }
    else {
      if ( prefix_lc>0 ) r = memcmp(k->text,(char *) b->keys+keyspace_lc-prefix_lc,(size_t)prefix_lc );

      if ( r==0 ) {
        klc = k->lc - prefix_lc;
        t = k->text + prefix_lc;
        high = b->keys_in_block-1;

        mid = (ix + high) / 2; 
        while ( ix<=high ) {
          switch ( compare_key(t,(UINT32)klc,b,mid) ) {
            case cmp_greater: 
              ix = mid + 1;
              mid = (ix + high) / 2;
              break;
            case cmp_equal:
              ix = mid;
              *found = true;
              high = -1;
              break;
            case cmp_less:
              high = mid - 1;
              mid = (ix + high) / 2;
              break;
          }
        }
      }
      else if ( r<0 ) ix = 0;
      else /* r>0 */ ix = b->keys_in_block;
    }
  }
  /* now ix points to first entry>=k or keys_in_block */
  return(ix);
}

/*
 * search_index searches index blocks down to stop_lvl and returns
 *   a pointer to the block at stop_lvl-1 in which the key lies.
 *   By construction, the key must be smaller than some key in
 *   each block searched unless it is in the rightmost block at
 *   this level.  If a key is larger than any in this level, then
 *   the last_pntr pointer is the returned.
 */

static struct leveln_pntr search_index(struct fcb *f, int index, UINT32 stop_lvl,
  struct key *k)
{struct leveln_pntr child; int ix,bufix; boolean done=false,found; char *name="search_index";

  child = f->first_at_level[f->primary_level[index]][index];
  if ( stop_lvl<=f->primary_level[index] )
    do {
      bufix = get_index(f,child);
      ix = search_block(f,bufix,k,&found);
      done = f->buffer_pool.buffer[bufix].b.level<=stop_lvl;
      if ( ix>=f->buffer_pool.buffer[bufix].b.keys_in_block ) { /* larger than any key */
        if ( null_pntr(f->buffer_pool.buffer[bufix].b.next) )
          child = f->last_pntr[f->buffer_pool.buffer[bufix].b.level][index];
	else {
          done = true;
          child = nulln_ptr;
		  set_error(f,max_key_err,"Search_index, key larger than any in block");
		  if ( log_errors ) {
				fprintf(f->log_file,", index_type=%d, stop_lvl=%d\n",index,stop_lvl);
		  }
	}
      }
      else unpackn_ptr(&(f->buffer_pool.buffer[bufix].b),ix,&child);
    }
    while ( !done );
  return(child);
}

/* file initialization */

static void set_block_shift(struct fcb *f)
{int i;

  i = block_lc;
  f->block_shift = 0;
  while (i>0) {
    i = i>>1;
    if ( i>0 ) f->block_shift++;
  }
}

static boolean machine_is_little_endian()
{int i=1; unsigned char *p=(unsigned char *)&i;

  if ( p[0]==1 ) return(true);
  else return(false);
}


/* init_key initializes the temporary part of the fcb and a few static  */
/*   variables.  It assumes that the fib has been initialized and that  */
/*   the endedness of the machine has been set.                         */

static void init_key(struct fcb *f, char id[], int lc)
{int i,j,hash_target,hash_blocks;;

  /*  printf("  version=%u, subversion=%d\n",f->version,f->sub_version);*/
  if ( f->version!=current_version || f->sub_version!=current_sub_version) { 
    f->error_code = version_err;
    return;
  }
  if ( !check_fcb(f) ) {
    f->error_code = filenotok_err;
    return;
  }
  if ( lc<(int)min_fcb_lc ) { f->error_code = smallfcb_err; return; }
  set_block_shift(f);
  j = 1;
  for (i=0; i<20; i++ ) {
    power_of_two[i] = j;
    j = j * 2;
  }
  f->trace = false; f->trace_freespace = false;
  f->log_file = NULL;
  f->log_file = stdout;
  f->open_file_cnt = 0;
  init_file_name(f,id);
  for (i=0; i<max_segment; i++) f->segment_ix[i] = max_files;
  f->current_age = 0;

  if ( lc==min_fcb_lc ) f->buffer_pool.buffers_allocated = min_buffer_cnt;
  else f->buffer_pool.buffers_allocated = min_buffer_cnt + (lc-min_fcb_lc) / buffer_lc;

  hash_target = f->buffer_pool.buffers_allocated * buf_hash_load_factor;
  hash_blocks = ((hash_target - 1) / hash_entries_per_buf) + 1;
  f->buffer_pool.buf_hash_table = (int *) &(f->buffer_pool.buffer[f->buffer_pool.buffers_allocated-hash_blocks]);
  f->buffer_pool.buf_hash_entries = hash_blocks * hash_entries_per_buf;
  f->buffer_pool.buffers_allocated = f->buffer_pool.buffers_allocated - hash_blocks;
  for (i=0; i<f->buffer_pool.buf_hash_entries; i++) f->buffer_pool.buf_hash_table[i] = -1;
  f->buffer_pool.buffers_in_use = 0;
  f->oldest_buffer = -1;
  f->youngest_buffer = -1;
  for (j=0; j<max_index; j++) {
    f->seq_cnt[j] = 0;
    for (i=0; i<max_level; i++) f->mru_at_level[i][j] = nulln_ptr;
  }
#ifdef log_buffers
  buffer_log = fopen("buffer_log","w");
#endif
}

/* intermediate calls */

/* extract_next extracts the key and pointer identified by the current file */
/*   position and advances the file position.  If max_key_lc==0 the key is  */
/*   not extracted.  Note that if the pointer contains the data_rec         */
/*   (lc<=f->max_data_in_index) then the data will extracted into rec.  The */
/*   caller should either point to the end destination for the data or      */
/*   point to the data_rec in p.  If max_rec_lc==0 no data will be          */
/*   extracted.                                                             */

static void extract_next(struct fcb *f, int index, int bufix, unsigned char t[], unsigned *key_lc,
 int max_key_lc, level0_pntr *p, unsigned char rec[], unsigned *rec_lc, unsigned max_rec_lc)
{struct key k;

  if ( f->position_ix[index]>=f->buffer_pool.buffer[bufix].b.keys_in_block ) {
    t[0] = '\0'; *key_lc = 0; *p = null0_ptr;
    if ( null_pntr(f->buffer_pool.buffer[bufix].b.next) ) f->error_code = ateof_err;
    else {
      set_error(f,ix_struct_err,"Error in extract_next");
    }
  }
  else {
    if ( max_key_lc>0 ) {
      get_nth_key(&(f->buffer_pool.buffer[bufix].b),&k,f->position_ix[index]);
      if ( k.lc<=max_key_lc ) *key_lc = k.lc;
      else {
        f->error_code = longkey_err; *key_lc = max_key_lc;
      }
      mvc(k.text,0,t,0,(unsigned)*key_lc);
    }
    unpack0_ptr_and_rec(f,&(f->buffer_pool.buffer[bufix]),f->position_ix[index],p,rec,rec_lc,(unsigned)max_rec_lc);
    if ( max_rec_lc==0 || max_rec_lc==f->data_in_index_lc ) /* not an error */;
    else if ( p->lc>(unsigned)*rec_lc ) f->error_code = longrec_err;
    f->position_ix[index]++;
    if ( f->position_ix[index]>=f->buffer_pool.buffer[bufix].b.keys_in_block && !null_pntr(f->buffer_pool.buffer[bufix].b.next) )
      set_position(f,index,f->buffer_pool.buffer[bufix].b.next,0);
  }
}

void kf_set_bof(struct fcb *f, int index)
{
  f->position[index] = f->first_at_level[0][index];
  f->position_ix[index] = 0;
  f->seq_cnt[index] = 0;
}

/* kf_get_ptr gets the pointer associated with key t. We've tried   */
/*   strategies for sequential access that look first in the        */
/*   current position block but they seem to work well only when    */
/*   we are accessing nearly every key.                             */

static int kf_get_rec(struct fcb *f, int index, unsigned char t[], unsigned key_lc, level0_pntr *p,
  unsigned char rec[], unsigned *rec_lc, unsigned max_rec_lc)
{struct leveln_pntr b,last_position; int ix=0,bufix=0,last_ix; unsigned lc; struct key k;
boolean found=false,seq=false;
 unsigned char t1[maxkey_lc]; char *name="kf_get_ptr";

  set_up(f,t,key_lc,&k);
  if ( f->error_code==no_err ) {
    last_position = f->position[index];
    last_ix = f->position_ix[index];
    b = search_index(f,index,level_one,&k);
    bufix = get_index(f,b);
    ix = search_block(f,bufix,&k,&found);
    set_position(f,index,b,ix);

    if ( (eqn_pntr(b,last_position) && ix>=last_ix) ) seq = true;
    else if ( eqn_pntr(f->buffer_pool.buffer[bufix].b.prev,last_position) ) seq = true;
    if ( seq ) {
      if ( f->seq_cnt[index]<INT_MAX ) (f->seq_cnt[index])++;
    }
    else f->seq_cnt[index] = 0;

    if ( found ) {
      extract_next(f,index,bufix,t1,&lc,0,p,rec,rec_lc,max_rec_lc);
    }
    else if ( f->error_code==no_err ) {
      f->error_code = getnokey_err; *p = null0_ptr;
    }
  }
  return(f->error_code);
}

static int kf_get_ptr(struct fcb *f,int index, unsigned char t[], unsigned key_lc,
  level0_pntr *p)
{int err; unsigned rec_lc;

  err = kf_get_rec(f,index,t,key_lc,p,p->data_rec,&rec_lc,f->data_in_index_lc);
  return(err);
}

/* Freespace management */

static int unpack_16bit(unsigned char key[], UINT16 *n)
{unsigned i;

  *n = 0;
  for (i=0; i<sizeof(UINT16); i++) {
    *n = (*n << 8) + key[i];
  }
  return(sizeof(UINT16));
}

static int unpack_32bit(unsigned char key[], UINT32 *n)
{unsigned i;

  *n = 0;
  for (i=0; i<sizeof(UINT32); i++) {
    *n = (*n << 8) + key[i];
  }
  return(sizeof(UINT32));
}

static int unpack_64bit(unsigned char key[], UINT64 *n)
{unsigned i;

  *n = 0;
  for (i=0; i<sizeof(UINT64); i++) {
    *n = (*n << 8) + key[i];
  }
  return(sizeof(UINT64));
}

/* unpack_lc_key extracts the lc, segment, and sc portions of key     */
/*   into p.                                                          */

int unpack_lc_key(unsigned char key[], level0_pntr *p)
{int lc; UINT16 segment; UINT32 plc; UINT64 sc;

  lc = unpack_32bit(key,&plc);
  p->lc = plc;
  lc = lc + unpack_16bit(key+lc,&segment);
  p->segment = segment;
  lc = lc + unpack_64bit(key+lc,&sc);
  p->sc = sc;
  return(lc);
}

int unpack_rec_key(unsigned char key[], level0_pntr *p)
{int lc; UINT16 segment; UINT64 sc;

  lc = unpack_16bit(key,&segment);
  p->segment = segment;
  lc = lc + unpack_64bit(key+lc,&sc);
  p->sc = sc;
  return(lc);
}

/* user callable entries */

int kf7_open_key(struct fcb *f, char id[], int lc, int read_only)
{  
  /*  read_fib(f,id,false,read_only);*/
  read_fib(f,id,machine_is_little_endian(),read_only);
  if ( f->error_code!=no_err ) set_error(f,badopen_err,"");
  else {
    init_key(f,id,lc);
    kf_set_bof(f,user_ix);
    kf_set_bof(f,free_rec_ix);
    kf_set_bof(f,free_lc_ix);
  }
  return(f->error_code);
}

int kf7_keyrec_lc(level0_pntr *p)
{
  if ( p->segment==max_segment && p->sc==0 ) return(p->lc);
  else if ( p->segment>=max_segment ) return(-1);
  else return(p->lc);
}


int kf7_get_ptr(struct fcb *f, unsigned char t[], unsigned key_lc, keyfile_pointer *pntr)
{int err;

  err = kf_get_ptr(f,user_ix,t,key_lc,pntr);
  return(err);
}


int kf7_get_rec(struct fcb *f, unsigned char t[], unsigned key_lc, unsigned char r[],
  int *rlc,int max_lc)
{level0_pntr dummy_p0;

  return( kf_get_rec(f,user_ix,t,key_lc,&dummy_p0,r,(unsigned *)rlc,(unsigned)max_lc) );
}

/* Functions to support subrecords: */

/* Multi version shell.  These calls are an interface to multiple    */
/*   keyed file versions.  Creates always create a keyed file in the */
/*   current version.  Opens read enough to the file to determine    */
/*   which open_key version to use.  Thereafter, the version in the  */
/*   fcb is used to determine which version of the API function to   */
/*   call.  If the version is not in the range of supported versions */
/*   then version_err is returned.  If the function called is not    */
/*   supported by the version opened then not_supported_err is       */
/*   returned.                                                       */

/* get_kf_version reads the version from the fcb.  This is not an    */
/*   exhaustive test (doesn't check marker or fcb_ok) since the      */
/*   appropriate open call will do more complete testing.            */

UINT32 get_kf_version(struct fcb *f, char id[])
{boolean err=false; UINT32 version=0; FILE *file;

  file = fopen(id,"rb");
  if ( file==NULL ) err = true;
  else {
    if ( fseeko(file,(FILE_OFFSET) 0,0)!=0 ) err = true;
    else {
      if ( fread(&version,sizeof(UINT32),(size_t)1,file)!=1 ) err = true;
      if ( fread(&version,sizeof(UINT32),(size_t)1,file)!=1 ) err = true;
      if ( machine_is_little_endian() ) byte_swap_UINT32((unsigned char *) &version);
    }
    fclose(file);
  }
  if ( err ) return(0);
  else return(version);
}


int open_key(struct fcb *f, char id[], int lc, int read_only)
{int err,version;

  version = get_kf_version(f,id);
  if ( version==7 )      err = kf7_open_key(f,id,lc,read_only);
  else                   err = version_err;
  return(err);
  
}

/* keyrec_lc to be removed when UMass can fix their code base */

int keyrec_lc(level0_pntr *p)
{int lc;

  lc = kf7_keyrec_lc(p);
  return(lc);
}


int get_ptr(struct fcb *f, char key[], int key_lc, keyfile_pointer *pntr)
{int err;

  if ( f->version==7 )      err = kf7_get_ptr(f,(unsigned char *)key,(unsigned)key_lc,pntr);
  else                      err = version_err;
  return(err);
}


int get_rec(struct fcb *f,char key[],int key_lc, char r[],int *rlc,int max_lc)
{int err;

  if ( f->version==7 )      err = kf7_get_rec(f,(unsigned char *)key,(unsigned)key_lc,
                                    (unsigned char *)r,rlc,max_lc);
  else                      err = version_err;
  return(err);
}

