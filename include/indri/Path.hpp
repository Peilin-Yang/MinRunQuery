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
// Path.hpp
//
// 20 May 2004 -- tds
//


#ifndef INDRI_PATH_HPP
#define INDRI_PATH_HPP

#include <string>
namespace indri
{
  namespace file
  {
    
    class Path {
    public:
      static std::string combine( const std::string& root, const std::string& addition );
    };
  }
}

#endif // INDRI_PATH_HPP

