/*==========================================================================
 * Copyright (c) 2001 Carnegie Mellon University.  All Rights Reserved.
 *
 * Use of the Lemur Toolkit for Language Modeling and Information Retrieval
 * is subject to the terms of the software license set forth in the LICENSE
 * file included with this software, and also available at
 * http://www.lemurproject.org/license.html
 *
 *==========================================================================
 */

/*
  10/18/2002 -- dmf Remove warning value exceeded int limit in compression
  from compress_ints because it is exercised to often with the termInfoList
  compression.
*/
#include "RVLCompress.hpp"

//
// _compress_bigger_int
//
// Contains the rare cases from compress_int, allowing compress_int
// to be inlined without a lot of code bloat.
//

char* lemur::utility::RVLCompress::_compress_bigger_int( char* dest, int data ) {
  if( data < (1<<21) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_TERMINATE( dest, data, 2 );
    return dest + 3;
  } else if( data < (1<<28) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_TERMINATE( dest, data, 3 );
    return dest + 4;
  } else {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_BYTE( dest, data, 3 );
    RVL_COMPRESS_TERMINATE( dest, data, 4 );
    return dest + 5;
  }
}

//
// _compress_bigger_longlong
//
//

char* lemur::utility::RVLCompress::_compress_bigger_longlong( char* dest, UINT64 data ) {
  if( data < (UINT64(1)<<21) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_TERMINATE( dest, data, 2 );
    return dest + 3;
  } else if( data < (UINT64(1)<<28) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_TERMINATE( dest, data, 3 );
    return dest + 4;
  } else if( data < (UINT64(1)<<35) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_BYTE( dest, data, 3 );
    RVL_COMPRESS_TERMINATE( dest, data, 4 );
    return dest + 5;
  } else if( data < (UINT64(1)<<42) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_BYTE( dest, data, 3 );
    RVL_COMPRESS_BYTE( dest, data, 4 );
    RVL_COMPRESS_TERMINATE( dest, data, 5 );
    return dest + 6;
  } else if( data < (UINT64(1)<<49) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_BYTE( dest, data, 3 );
    RVL_COMPRESS_BYTE( dest, data, 4 );
    RVL_COMPRESS_BYTE( dest, data, 5 );
    RVL_COMPRESS_TERMINATE( dest, data, 6 );
    return dest + 7;
  } else if( data < (UINT64(1)<<56) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_BYTE( dest, data, 3 );
    RVL_COMPRESS_BYTE( dest, data, 4 );
    RVL_COMPRESS_BYTE( dest, data, 5 );
    RVL_COMPRESS_BYTE( dest, data, 6 );
    RVL_COMPRESS_TERMINATE( dest, data, 7 );
    return dest + 8;
  } else if( data < (UINT64(1)<<63) ) {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_BYTE( dest, data, 3 );
    RVL_COMPRESS_BYTE( dest, data, 4 );
    RVL_COMPRESS_BYTE( dest, data, 5 );
    RVL_COMPRESS_BYTE( dest, data, 6 );
    RVL_COMPRESS_BYTE( dest, data, 7 );
    RVL_COMPRESS_TERMINATE( dest, data, 8 );
    return dest + 9;
  } else {
    RVL_COMPRESS_BYTE( dest, data, 0 );
    RVL_COMPRESS_BYTE( dest, data, 1 );
    RVL_COMPRESS_BYTE( dest, data, 2 );
    RVL_COMPRESS_BYTE( dest, data, 3 );
    RVL_COMPRESS_BYTE( dest, data, 4 );
    RVL_COMPRESS_BYTE( dest, data, 5 );
    RVL_COMPRESS_BYTE( dest, data, 6 );
    RVL_COMPRESS_BYTE( dest, data, 7 );
    RVL_COMPRESS_BYTE( dest, data, 8 );
    RVL_COMPRESS_TERMINATE( dest, data, 9 );
    return dest + 10;
  }
}
