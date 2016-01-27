/*==========================================================================
 * Copyright (c) 2004 University of Massachusetts.  All Rights Reserved.
 *
 * Use of the Lemur Toolkit for Language Modeling and Information Retrieval
 * is subject to the terms of the software license set forth in the LICENSE
 * file included with this software, and also available at
 * http://www.lemurproject.org/license.html
 *
 *==========================================================================
 */


//
// Path.cpp
//
// 20 May 2004 -- tds
//

#include "indri/Path.hpp"

#ifdef WIN32
#define PATH_SEPARATOR '\\'
#else
#define PATH_SEPARATOR '/'
#endif

static int path_last_separator( const std::string& path ) {
  int i;

  // skip any trailing slashes
  for( i=(int)path.size()-1; i>=0; i-- ) {
    if( path[i] != PATH_SEPARATOR )
      break;
  }

  for( ; i>=0; i-- ) {
    if( path[i] == PATH_SEPARATOR )
      break;
  }

  return i;
}

std::string indri::file::Path::combine( const std::string& root, const std::string& addition ) {
  if( !root.size() )
    return addition;

  if( *(root.end()-1) == PATH_SEPARATOR )
    return root + addition;

  return root + PATH_SEPARATOR + addition;
}
