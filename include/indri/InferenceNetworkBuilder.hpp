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
// InferenceNetworkBuilder
//
// 8 March 2004 -- tds
//

#ifndef INDRI_INFERENCENETWORKBUILDER_HPP
#define INDRI_INFERENCENETWORKBUILDER_HPP

#include "indri/QuerySpec.hpp"
#include "indri/Repository.hpp"
#include "indri/InferenceNetwork.hpp"
#include "indri/ListCache.hpp"
#include <map>
#include <vector>
namespace indri
{
  namespace infnet
  {
    
    class InferenceNetworkBuilder : public indri::lang::Walker {
    private:
      std::map< indri::lang::Node*, InferenceNetworkNode* > _nodeMap;
      InferenceNetwork* _network;
      indri::collection::Repository& _repository;
      indri::lang::ListCache& _cache;
      int _resultsRequested;
      int _maxWildcardTerms;

      template<typename _To, typename _From>
      std::vector<_To*> _translate( const std::vector<_From*>& children ) {
        std::vector<_To*> translation;

        for( unsigned int i=0; i<children.size(); i++ ) {
          translation.push_back( dynamic_cast<_To*>(_nodeMap[children[i]]) );  
        }

        return translation;
      }

      indri::query::TermScoreFunction* _buildTermScoreFunction( const std::string& smoothing, double occurrences, double contextSize, int documentOccurrences, int documentCount ) const;
    public:
      static const int DEFAULT_MAX_WILDCARD_TERMS = 100;

      InferenceNetworkBuilder( indri::collection::Repository& repository, indri::lang::ListCache& cache, int resultsRequested, int maxWildcardTerms=DEFAULT_MAX_WILDCARD_TERMS );
      ~InferenceNetworkBuilder();

      InferenceNetwork* getNetwork();

      void defaultAfter( indri::lang::Node* node );
      void after( indri::lang::ContextSimpleCounterNode* contextSimpleCounterNode );
      void after( indri::lang::ScoreAccumulatorNode* scoreAccumulatorNode );
      void after( indri::lang::TermFrequencyScorerNode* termScorerNode );
      void after( indri::lang::RawScorerNode* rawScorerNode );
      void after( indri::lang::WeightNode* weightNode );
      void after( indri::lang::CombineNode* combineNode );
    };
  }
}

#endif // INDRI_INFERENCENETWORKBUILDER


