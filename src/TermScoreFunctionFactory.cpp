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
// TermScoreFunctionFactory
//
// 19 August 2004 -- tds
//

#include "indri/TermScoreFunctionFactory.hpp"
#include "indri/TermScoreFunction.hpp"
#include "indri/BeliefNode.hpp"
#include "indri/Parameters.hpp"
#include <map>
 
indri::query::TermScoreFunction* indri::query::TermScoreFunctionFactory::get( const std::string& stringSpec, double occurrences, 
    double contextSize, int documentOccurrences, int documentCount ) {
  // this is something that never happens in our collection, so we assume that it
  // happens somewhat less often than 1./collectionSize.  I picked 1/(2*collectionSize)
  // because it seemed most appropriate (from InferenceNetworkBuilder)
  if (contextSize == 0) contextSize = 1; // For non-existant fields.
  
  double collectionFrequency = occurrences ? (occurrences/contextSize) :
    (collectionFrequency = 1.0 / double(contextSize*2.));

  std::map<std::string, double> paras;
  return new indri::query::TermScoreFunction( collectionFrequency, contextSize, documentOccurrences, documentCount, paras );
}


