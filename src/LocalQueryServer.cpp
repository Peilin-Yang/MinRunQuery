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

#include "indri/UnnecessaryNodeRemoverCopier.hpp"
#include "indri/ContextSimpleCountCollectorCopier.hpp"
#include "indri/FrequencyListCopier.hpp"
#include "indri/DagCopier.hpp"

#include "indri/InferenceNetworkBuilder.hpp"
#include "indri/InferenceNetwork.hpp"

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

indri::server::LocalQueryServer::LocalQueryServer( indri::collection::Repository& repository ) :
  _repository(repository), _maxWildcardMatchesPerTerm(indri::infnet::InferenceNetworkBuilder::DEFAULT_MAX_WILDCARD_TERMS)
{
  // if supplied and false, turn off optimization for all queries.
  _optimizeParameter = indri::api::Parameters::instance().get( "optimize", true );
}

//
// _indexWithDocument
//

indri::index::Index* indri::server::LocalQueryServer::_indexWithDocument( indri::collection::Repository::index_state& indexes, lemur::api::DOCID_T documentID ) {
  for( size_t i=0; i<indexes->size(); i++ ) {
    indri::thread::ScopedLock lock( (*indexes)[i]->statisticsLock() );
    lemur::api::DOCID_T lowerBound = (*indexes)[i]->documentBase();
    lemur::api::DOCID_T upperBound = (*indexes)[i]->documentMaximum();
    
    if( lowerBound <= documentID && upperBound > documentID ) {
      return (*indexes)[i];
    }
  }
  
  return 0;
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

indri::server::QueryServerResponse* indri::server::LocalQueryServer::runQuery( std::vector<indri::lang::Node*>& roots, int resultsRequested, bool optimize ) {

  indri::lang::TreePrinterWalker printer;

  // use UnnecessaryNodeRemover to get rid of window nodes, ExtentAnd nodes and ExtentOr nodes
  // that only have one child and LengthPrior nodes where the exponent is zero
  indri::lang::ApplyCopiers<indri::lang::UnnecessaryNodeRemoverCopier> unnecessary( roots );

  // run the contextsimplecountcollectorcopier to gather easy stats
  indri::lang::ApplyCopiers<indri::lang::ContextSimpleCountCollectorCopier> contexts( unnecessary.roots(), _repository );

  // use frequency-only nodes where appropriate
  indri::lang::ApplyCopiers<indri::lang::FrequencyListCopier> frequency( contexts.roots(), _cache );

  // fold together any nested weight nodes
  indri::lang::ApplyCopiers<indri::lang::WeightFoldingCopier> weight( frequency.roots() );

  // make all this into a dag
  indri::lang::ApplySingleCopier<indri::lang::DagCopier> dag( weight.roots(), _repository );

  std::vector<indri::lang::Node*>& networkRoots = dag.roots();
  // turn off optimization if called with optimize == false
  // turn off optimization if called the Parameter optimize == false
  if( !optimize || !_optimizeParameter ) {
    // we may be asked not to perform optimizations that might
    // drastically change the structure of the tree; for instance,
    // annotation queries may ask for this
    networkRoots = contexts.roots();
  }
  /*
    indri::lang::TreePrinterWalker printer;
    indri::lang::ApplyWalker<indri::lang::TreePrinterWalker> printTree(networkRoots, &printer);
  */

  // build an inference network
  indri::infnet::InferenceNetworkBuilder builder( _repository, _cache, resultsRequested, _maxWildcardMatchesPerTerm );
  indri::lang::ApplyWalker<indri::infnet::InferenceNetworkBuilder> buildWalker( networkRoots, &builder );

  indri::infnet::InferenceNetwork* network = builder.getNetwork();
  indri::infnet::InferenceNetwork::MAllResults result;
  result = network->evaluate();

  return new indri::server::LocalQueryServerResponse( result );
}
