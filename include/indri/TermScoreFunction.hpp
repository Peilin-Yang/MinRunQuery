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

#include <string>
#include <map>

namespace indri
{
  namespace query
  {
    class TermScoreFunction {
    private:
      double _collectionOccurence;
      double _collectionSize;
      double _documentOccurrences;
      double _documentCount;
      double _queryLength;
      std::map<std::string, double> _modelParas;
    public:
      TermScoreFunction( double collectionOccurence, double collectionSize, double documentOccurrences, 
          double documentCount, double queryLength, std::map<std::string, double>& paras );
      void _preCompute();
      double scoreOccurrence( double occurrences, int contextLength, double qtf );
    };
  }
}

#endif // INDRI_TERMSCOREFUNCTION_HPP

