
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
// relevancemodel
//
// 23 June 2005 -- tds
//

#include "indri/RelevanceModel.hpp"
#include <math.h>

//
// RelevanceModel
//

indri::query::RelevanceModel::RelevanceModel( 
                  indri::api::QueryEnvironment& environment,
                  const std::string& smoothing,
                  int maxGrams,
                  int documents )
  :
  _environment(environment),
  _smoothing(smoothing),
  _documents(documents),
  _maxGrams(maxGrams)
{
}

//
// ~RelevanceModel
//

indri::query::RelevanceModel::~RelevanceModel() {
  HGram::iterator iter;

  for( iter = _gramTable.begin(); iter != _gramTable.end(); iter++ ) {
    delete *(iter->second);
  }
}

//
// getGrams
//

const std::vector<indri::query::RelevanceModel::Gram*>& indri::query::RelevanceModel::getGrams() const {
  return _grams;
}

//
// getQueryResults
//

const std::vector<indri::api::ScoredExtentResult>& indri::query::RelevanceModel::getQueryResults() const {
  return _results;
}

//
// _extractDocuments
//

void indri::query::RelevanceModel::_extractDocuments() {
  for( size_t i=0; i<_results.size(); i++ ) {
    _documentIDs.push_back( _results[i].document );
  }
}

