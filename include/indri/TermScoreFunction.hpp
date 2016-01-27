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
// TermScoreFunction
//
// 23 January 2004 -- tds
//

#ifndef INDRI_TERMSCOREFUNCTION_HPP
#define INDRI_TERMSCOREFUNCTION_HPP

#include <map>

namespace indri
{
  namespace query
  {
    class TermScoreFunction {
    private:
      std::map<std::string, double> _modelParas;
    public:
      TermScoreFunction( double collectionFrequency, double collectionSize, double documentOccurrences, 
          double documentCount, std::map<std::string, double>& paras );
      double scoreOccurrence( double occurrences, int contextLength );
      double scoreOccurrence( double occurrences, int contextLength, double documentOccurrences, int documentLength );
    };
  }
}

#endif // INDRI_TERMSCOREFUNCTION_HPP

