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
// LocalQueryServer
//
// 15 March 2004 -- tds
//

#include "indri/LocalQueryServer.hpp"
#include "indri/QuerySpec.hpp"
#include "lemur/lemur-platform.h"
#include "lemur/lemur-compat.hpp"
#include <vector>

// #include "indri/UnnecessaryNodeRemoverCopier.hpp"
// #include "indri/ContextSimpleCountCollectorCopier.hpp"
// #include "indri/FrequencyListCopier.hpp"
// #include "indri/DagCopier.hpp"

#include "indri/InferenceNetworkBuilder.hpp"
#include "indri/InferenceNetwork.hpp"
#include "indri/ContextSimpleCountAccumulator.hpp"

#include "indri/CompressedCollection.hpp"
#include "indri/delete_range.hpp"
#include "indri/WeightFoldingCopier.hpp"

#include "indri/Appliers.hpp"
#include "indri/ScopedLock.hpp"

#include "indri/TreePrinterWalker.hpp"


//
// Response objects
//
namespace indri
{
  namespace server
  {
    
    class LocalQueryServerResponse : public QueryServerResponse {
    private:
      indri::infnet::InferenceNetwork::MAllResults _results;

    public:
      LocalQueryServerResponse( const indri::infnet::InferenceNetwork::MAllResults& results ) :
        _results(results) {
      }
  
      indri::infnet::InferenceNetwork::MAllResults& getResults() {
        return _results;
      }
    };

    class LocalQueryServerMetadataResponse : public QueryServerMetadataResponse {
    private:
      std::vector<std::string> _metadata;

    public:
      LocalQueryServerMetadataResponse( const std::vector<std::string>& metadata ) :
        _metadata(metadata)
      {
      }

      std::vector<std::string>& getResults() {
        return _metadata;
      }
    };
  }
}

//
// Class code
//

indri::server::LocalQueryServer::LocalQueryServer( indri::collection::Repository& repository ) : _repository(repository)
{
  // if supplied and false, turn off optimization for all queries.
  _optimizeParameter = indri::api::Parameters::instance().get( "optimize", true );
}

//
// document
//

std::string indri::server::LocalQueryServer::documentMetadatum( lemur::api::DOCID_T documentID, const std::string& attributeName ) {
  indri::collection::CompressedCollection* collection = _repository.collection();
  return collection->retrieveMetadatum( documentID, attributeName );
}

indri::server::QueryServerMetadataResponse* indri::server::LocalQueryServer::documentMetadata( const std::vector<lemur::api::DOCID_T>& documentIDs, const std::string& attributeName ) {
  std::vector<std::string> result;

  std::vector<std::pair<lemur::api::DOCID_T, int> > docSorted;
  for( size_t i=0; i<documentIDs.size(); i++ ) {
    docSorted.push_back( std::make_pair( documentIDs[i], i ) );
  }
  std::sort( docSorted.begin(), docSorted.end() );

  for( size_t i=0; i<docSorted.size(); i++ ) {
    result.push_back( documentMetadatum(docSorted[i].first, attributeName) );
  }

  std::vector<std::string> actual;
  actual.resize( documentIDs.size() );
  for( size_t i=0; i<docSorted.size(); i++ ) {
    actual[docSorted[i].second] = result[i];
  }

  return new indri::server::LocalQueryServerMetadataResponse( actual );
}

std::string indri::server::LocalQueryServer::processTerm( std::string s) {
  std::string processed_term = _repository.processTerm(s);
  return processed_term;
}

indri::server::QueryServerResponse* indri::server::LocalQueryServer::getGlobalStatistics( std::vector<std::string>& queryTerms ) {
  indri::infnet::InferenceNetwork* network = new indri::infnet::InferenceNetwork(_repository);
  for (size_t i = 0; i != queryTerms.size(); i++) {
    indri::infnet::ContextSimpleCountAccumulator *contextCount = new indri::infnet::ContextSimpleCountAccumulator( queryTerms[i] );
    network->addEvaluatorNode( contextCount );
  }
  
  indri::infnet::InferenceNetwork::MAllResults result;
  result = network->evaluate();

  return new indri::server::LocalQueryServerResponse( result );  
}

indri::server::QueryServerResponse* indri::server::LocalQueryServer::runQuery( 
    std::map<std::string, double>& queryDict, 
    int resultsRequested, 
    bool optimize ) {
  indri::infnet::InferenceNetwork* network = new indri::infnet::InferenceNetwork(_repository);
  indri::infnet::BeliefNode* belief = 0;
  indri::query::TermScoreFunction* function = 0;
/* 
  function = indri::query::TermScoreFunctionFactory::get( smoothing, occurrences, contextSize, documentOccurrences, documentCount );
                                      ( termScorerNode->getSmoothing(),
                                      termScorerNode->getOccurrences(),
                                      termScorerNode->getContextSize(),
                                      termScorerNode->getDocumentOccurrences(),
                                      termScorerNode->getDocumentCount());

  if( termScorerNode->getOccurrences() > 0 ) {
    bool stopword = false;
    std::string processed = termScorerNode->getText();
    int termID = 0;
  
    // stem and stop the word
    if( termScorerNode->getStemmed() == false ) {
      processed = _repository.processTerm( termScorerNode->getText() );
      stopword = processed.length() == 0;
    }

    // if it isn't a stopword, we can try to get it from the index
    if( !stopword ) {
      int listID = _network->addDocIterator( processed );
      belief = new TermFrequencyBeliefNode( termScorerNode->nodeName(), *_network, listID, *function );
    }
  }

  // either there's no list here, or there aren't any occurrences
  // in the local collection, so just use a NullScorerNode in place
  if( !belief ) {
    belief = new NullScorerNode( termScorerNode->nodeName(), *function );
  }

  network->addScoreFunction( function );
  network->addBeliefNode( belief );*/

  indri::infnet::InferenceNetwork::MAllResults result;
  result = network->evaluate();

  return new indri::server::LocalQueryServerResponse( result );
}
