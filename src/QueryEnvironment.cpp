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
// QueryEnvironment
//
// 9 March 2004 -- tds
//
#include "indri/indri-platform.h"
#include "indri/QueryEnvironment.hpp"
#include "indri/CompressedCollection.hpp"

#include <vector>
#include "indri/delete_range.hpp"

#include "indri/InferenceNetwork.hpp"
#include "indri/QuerySpec.hpp"
#include "indri/ScoredExtentResult.hpp"

#include "indri/LocalQueryServer.hpp"
#include "indri/NetworkServerProxy.hpp"
#include "indri/NetworkStream.hpp"
#include "indri/NetworkMessageStream.hpp"

#include "indri/DagCopier.hpp"
#include "indri/InferenceNetworkBuilder.hpp"
#include "indri/RawScorerNodeExtractor.hpp"
#include "indri/ContextSimpleCountCollectorCopier.hpp"
#include "indri/TreePrinterWalker.hpp"
#include "indri/ContextCountGraphCopier.hpp"
#include "indri/SmoothingAnnotatorWalker.hpp"
#include "indri/ExtentRestrictionModelAnnotatorCopier.hpp"

#include "indri/InferenceNetworkBuilder.hpp"
#include "indri/Packer.hpp"
#include "indri/Unpacker.hpp"

#include "indri/XMLReader.hpp"
#include "indri/IndriTimer.hpp"

#include "indri/Index.hpp"

#include <map>

#include "indri/Appliers.hpp"
#include "indri/TreePrinterWalker.hpp"

#include "indri/VocabularyIterator.hpp"

#include "indri/SimpleQueryParser.hpp"

using namespace lemur::api;

#define TIME_QUERIES
// debug code: should be gone soon
#ifdef TIME_QUERIES
#define INIT_TIMER      indri::utility::IndriTimer t; t.start();
#define PRINT_TIMER(s)  { t.printElapsedMicroseconds( std::cout ); std::cout << ": " << s << std::endl; }
#else
#define INIT_TIMER
#define PRINT_TIMER(s)
#endif

// for debugging; this class prints a query tree
namespace indri
{
  namespace lang
  {
    class Printer : public indri::lang::Walker {
    private:
      int tabs;
    public:
      Printer() : tabs(0) {}

      void defaultBefore( indri::lang::Node* n ) {
        for( int i=0; i<tabs; i++ )
          std::cout << "\t";
        std::cout << n->typeName() << " " << n->nodeName() << " " << n->queryText() << std::endl;
        tabs++;
      }

      void defaultAfter( indri::lang::Node* n ) {
        tabs--;
      }

    };
  }
}

//
// Helper document methods
//

// split a document ID list into sublists, one for each query server
void qenv_scatter_document_ids( const std::vector<DOCID_T>& documentIDs, std::vector< std::vector<DOCID_T> >& docIDLists, std::vector< std::vector<DOCID_T> >& docIDPositions, int serverCount ) {
  docIDLists.resize( serverCount );
  docIDPositions.resize( serverCount );

  for( unsigned int i=0; i<documentIDs.size(); i++ ) {
    DOCID_T id = documentIDs[i];
    int serverID = id % serverCount;

    docIDLists[serverID].push_back( id / serverCount );
    docIDPositions[serverID].push_back(i);
  }
}

// retrieve a list of results from each query server, and fold those results into a master list
template<class _ResponseType, class _ResultType>
void qenv_gather_document_results( const std::vector< std::vector<DOCID_T> >& docIDLists,
                                   const std::vector< std::vector<DOCID_T> >& docIDPositions,
                                   indri::utility::greedy_vector<_ResponseType>& responses,
                                   std::vector<_ResultType>& results ) {
  for( size_t i=0; i<docIDLists.size(); i++ ) {
    if( docIDLists[i].size() ) {
      std::vector<_ResultType> serverResult = responses[i]->getResults();
      delete responses[i];
      
      // fold the document names back into one large result list
      for( size_t j=0; j<docIDLists[i].size(); j++ ) {
        int resultIndex = docIDPositions[i][j];
        results[ resultIndex ] = serverResult[j];
      }
    }
  }
}

//
// QueryEnvironment definition
//

indri::api::QueryEnvironment::QueryEnvironment(){}

indri::api::QueryEnvironment::~QueryEnvironment() {
  close();
}

void indri::api::QueryEnvironment::setMemory( UINT64 memory ) {
  _parameters.set( "memory", memory );
}

void indri::api::QueryEnvironment::setSingleBackgroundModel( bool background ) {
  _parameters.set( "singleBackgroundModel", background );
}

void indri::api::QueryEnvironment::setScoringRules( const std::vector<std::string>& rules ) {
  _parameters.set("rule","");
  for( unsigned int i=0; i<rules.size(); i++ ) {
    _parameters.append("rule").set( rules[i] );
  }
}

void indri::api::QueryEnvironment::setStopwords( const std::vector<std::string>& stopwords ) {
  _parameters.set("stopper","");
  Parameters p = _parameters.get("stopper");
  for( unsigned int i=0; i<stopwords.size(); i++ ) {
    p.append("word").set(stopwords[i]);
  }
}

void indri::api::QueryEnvironment::_setQTF(std::map<std::string, double>& parsedQuery) {
  _queryDict.clear();
  for (std::map<std::string, double>::iterator it = parsedQuery.begin(); it != parsedQuery.end(); it++) {
    QueryDict qd;
    qd.qtf = it->second;
    _queryDict[it->first] = qd;
  }
}

void indri::api::QueryEnvironment::_copyStatistics( std::vector<indri::lang::RawScorerNode*>& scorerNodes, 
	indri::infnet::InferenceNetwork::MAllResults& statisticsResults ) {
  for( size_t i=0; i<scorerNodes.size(); i++ ) {
    std::vector<ScoredExtentResult>& occurrencesList = statisticsResults[ scorerNodes[i]->nodeName() ][ "occurrences" ];
    std::vector<ScoredExtentResult>& contextSizeList = statisticsResults[ scorerNodes[i]->nodeName() ][ "contextSize" ];
    std::vector<ScoredExtentResult>& documentOccurrencesList = statisticsResults[ scorerNodes[i]->nodeName() ][ "documentOccurrences" ];
    std::vector<ScoredExtentResult>& documentCountList = statisticsResults[ scorerNodes[i]->nodeName() ][ "documentCount" ];

    double occurrences = occurrencesList[0].score;
    double contextSize = contextSizeList[0].score;
    int documentOccurrences = int(documentOccurrencesList[0].score);
    int documentCount = int(documentCountList[0].score);

    scorerNodes[i]->setStatistics( occurrences, contextSize, documentOccurrences, documentCount );
  }
}

// run a query (Indri query language)
std::vector<indri::api::ScoredExtentResult> indri::api::QueryEnvironment::_runQuery( indri::infnet::InferenceNetwork::MAllResults& results,
                                                                                     const std::string& q,
                                                                                     int resultsRequested,
                                                                                     const std::string &queryType) {
  INIT_TIMER
  PRINT_TIMER( "Initialization complete" );

  indri::query::SimpleQueryParser* sqp = new indri::query::SimpleQueryParser();
  std::map<std::string, double> parsedQuery = sqp->parseQuery( q );
  _setQTF(parsedQuery);

  PRINT_TIMER( "Parsing complete" );
  

  // push down language models from ExtentRestriction nodes
  /*  
  indri::lang::ExtentRestrictionModelAnnotatorCopier restrictionCopier;
  rootNode = dynamic_cast<indri::lang::ScoredExtentNode*>(rootNode->copy(restrictionCopier));

  // extract the raw scorer nodes from the query tree
  indri::lang::RawScorerNodeExtractor extractor;
  rootNode->walk(extractor);

  // copy out a new graph that has context counters in it -- this will be evaluated
  // so that we can get counts for everything in the query.  We need those counts
  // so that we can score the query terms correctly.
  std::vector<indri::lang::RawScorerNode*>& scorerNodes = extractor.getScorerNodes();
  indri::infnet::InferenceNetwork::MAllResults statisticsResults;
            
  indri::lang::ApplyCopiers<indri::lang::ContextCountGraphCopier, indri::lang::RawScorerNode> graph( scorerNodes );*/
  //indri::infnet::InferenceNetwork::MAllResults statisticsResults;
  //_sumServerQuery( statisticsResults, resultsRequested ); //1000

  //PRINT_TIMER( "Statistics complete" );

  // feed the statistics we found back into the query network
  //_copyStatistics( scorerNodes, statisticsResults );

  /*
  // annotate the graph with smoothing parameters
  indri::lang::SmoothingAnnotatorWalker smoother( _parameters );
  rootNode->walk(smoother);
  
  // run a scored query (possibly including a document set)
  std::string accumulatorName;
  _scoredQuery( results, rootNode, accumulatorName, resultsRequested );
  std::vector<indri::api::ScoredExtentResult> queryResults = results[accumulatorName]["scores"];
  std::stable_sort( queryResults.begin(), queryResults.end(), indri::api::ScoredExtentResult::score_greater() );
  if( (int)queryResults.size() > resultsRequested )
    queryResults.resize( resultsRequested );

  PRINT_TIMER( "Query complete" );

  delete(sqp);*/
  std::string accumulatorName;
  _scoredQuery( results, accumulatorName, resultsRequested );
  std::vector<indri::api::ScoredExtentResult> queryResults = results[accumulatorName]["scores"];

  //std::vector<indri::api::ScoredExtentResult> queryResults;
  return queryResults;
}

//
// Runs a query in parallel across all servers, and returns a vector of responses.
// This method will block until all responses have been received.
//

std::vector<indri::server::QueryServerResponse*> indri::api::QueryEnvironment::_runServerQuery( int resultsRequested ) {
  std::vector<indri::server::QueryServerResponse*> responses;
  
  // this ships out the requests to each server (doesn't necessarily block until they're done)
  for( size_t i=0; i<_servers.size(); i++ ) {
    //indri::server::QueryServerResponse* response = _servers[i]->getGlobalStatistics( _queryDict );
    //responses.push_back( response );
  }

  // this just goes through all the results, blocking on each one,
  // making sure they've all arrived
  for( size_t i=0; i<_servers.size(); i++ ) {
    responses[i]->getResults();
  }

  return responses;
}

// 
// This method is used for combining raw scores from ContextCounterNodes.
//

void indri::api::QueryEnvironment::_sumServerQuery( indri::infnet::InferenceNetwork::MAllResults& results, int resultsRequested ) {
  std::vector<indri::server::QueryServerResponse*> serverResults = _runServerQuery( resultsRequested );
  results.clear();

  indri::infnet::InferenceNetwork::MAllResults::iterator nodeIter;
  indri::infnet::EvaluatorNode::MResults::iterator listIter;

  for( size_t i=0; i<serverResults.size(); i++ ) {
    indri::server::QueryServerResponse* response = serverResults[i];
    indri::infnet::InferenceNetwork::MAllResults& machineResults = response->getResults();
    indri::infnet::InferenceNetwork::MAllResults::iterator iter;

    for( nodeIter = machineResults.begin(); nodeIter != machineResults.end(); nodeIter++ ) {
      for( listIter = nodeIter->second.begin(); listIter != nodeIter->second.end(); listIter++ ) {
        std::vector<indri::api::ScoredExtentResult>& currentResultList = results[ nodeIter->first ][ listIter->first ];
        std::vector<indri::api::ScoredExtentResult>& machineResultList = listIter->second;

        if( currentResultList.size() == 0 ) {
          currentResultList.assign( machineResultList.begin(), machineResultList.end() );
        } else {
          assert( machineResultList.size() == currentResultList.size() );

          for( size_t i=0; i<machineResultList.size(); i++ ) {
            currentResultList[i].score += machineResultList[i].score;
          }
        }
      }
    }
  }

  indri::utility::delete_vector_contents<indri::server::QueryServerResponse*>( serverResults );
}

void indri::api::QueryEnvironment::_mergeQueryResults( indri::infnet::InferenceNetwork::MAllResults& results, 
	std::vector<indri::server::QueryServerResponse*>& responses ) {
  results.clear();

  indri::infnet::InferenceNetwork::MAllResults::iterator nodeIter;
  indri::infnet::EvaluatorNode::MResults::iterator listIter;

  // merge all the results from these machines into one master list
  for( size_t i=0; i<responses.size(); i++ ) {
    indri::server::QueryServerResponse* response = responses[i];
    indri::infnet::InferenceNetwork::MAllResults& machineResults = response->getResults();

    for( nodeIter = machineResults.begin(); nodeIter != machineResults.end(); nodeIter++ ) {
      indri::infnet::EvaluatorNode::MResults& node = nodeIter->second;

      for( listIter = node.begin(); listIter != node.end(); listIter++ ) {
        const std::vector<indri::api::ScoredExtentResult>& partialResultList = listIter->second;
        std::vector<indri::api::ScoredExtentResult>& totalResultList = results[ nodeIter->first ][ listIter->first ];

        for( size_t j=0; j<partialResultList.size(); j++ ) {
          indri::api::ScoredExtentResult singleResult = partialResultList[j];
          singleResult.document = (singleResult.document*int(_servers.size())) + int(i);
          totalResultList.push_back( singleResult );
        }
      }
    }
  }
}

//
// This method is used to merge document results from multiple servers.  It does this
// by reassigning document IDs with the following function:
//      serverCount = _servers.size();
//      cookedDocID = rawDocID * serverCount + docServer;
// So, for document 6 from server 3 (out of 7 servers), the cooked docID would be:
//      (6 * 7) + 3 = 45.
// This function has the nice property that if there is only one server running,
// cookedDocID == rawDocID.
//

void indri::api::QueryEnvironment::_mergeServerQuery( 
	indri::infnet::InferenceNetwork::MAllResults& results, 
	int resultsRequested ) {
  std::vector<indri::server::QueryServerResponse*> serverResults = _runServerQuery( resultsRequested );
  results.clear();

  indri::infnet::InferenceNetwork::MAllResults::iterator nodeIter;
  indri::infnet::EvaluatorNode::MResults::iterator listIter;

  _mergeQueryResults( results, serverResults );

  // now, for each node, sort the result list, and trim off any results past the
  // requested amount
  for( nodeIter = results.begin(); nodeIter != results.end(); nodeIter++ ) {
    for( listIter = nodeIter->second.begin(); listIter != nodeIter->second.end(); listIter++ ) {
      std::vector<indri::api::ScoredExtentResult>& listResults = listIter->second;
      std::stable_sort( listResults.begin(), listResults.end(), indri::api::ScoredExtentResult::score_greater() );

      if( int(listResults.size()) > resultsRequested )
        listResults.resize( resultsRequested );
    }
  }

  indri::utility::delete_vector_contents<indri::server::QueryServerResponse*>( serverResults );
}

//
// addIndex
//

void indri::api::QueryEnvironment::addIndex( const std::string& pathname ) {
  std::map<std::string, std::pair<indri::server::QueryServer *, indri::collection::Repository *> >::iterator iter;
  iter = _repositoryNameMap.find(pathname);
  if (iter == _repositoryNameMap.end()) { // only add if not present
    indri::collection::Repository* repository = new indri::collection::Repository();
    repository->openRead( pathname, &_parameters );
    _repositories.push_back( repository );
    
    indri::server::LocalQueryServer *server = new indri::server::LocalQueryServer( *repository ) ;
    _servers.push_back( server );
    _repositoryNameMap[pathname] = std::make_pair(server, repository);
  } // else, could throw an Exception, as it is a logical error.
}

//
// addServer
//

void indri::api::QueryEnvironment::addServer( const std::string& hostname ) {
  std::map<std::string, std::pair<indri::server::QueryServer *, indri::net::NetworkStream *> >::iterator iter;
  iter = _serverNameMap.find(hostname);
  if (iter == _serverNameMap.end()) { // only add if not present

    indri::net::NetworkStream* stream = new indri::net::NetworkStream;
    unsigned int port = INDRID_PORT;
    std::string host = hostname;
    int colon = (int)hostname.find(':');

    if( colon > 0 ) {
      host = hostname.substr( 0, colon );
      port = atoi( hostname.substr( colon+1 ).c_str() );
    }

    if( !stream->connect( host.c_str(), port ) ) {
      delete stream;
      throw Exception( "QueryEnvironment", "Failed to connect to server" );
    }

    _streams.push_back( stream );
    indri::net::NetworkMessageStream* messageStream = new indri::net::NetworkMessageStream( stream );
    indri::server::NetworkServerProxy* proxy = new indri::server::NetworkServerProxy( messageStream );

    _messageStreams.push_back( messageStream );
    _servers.push_back( proxy );
    _serverNameMap[hostname] = std::make_pair(proxy, stream);
  }
}

void indri::api::QueryEnvironment::close() {
  indri::utility::delete_vector_contents<indri::server::QueryServer*>( _servers );
  _servers.clear();
  indri::utility::delete_vector_contents<indri::net::NetworkMessageStream*>( _messageStreams );
  _messageStreams.clear();
  indri::utility::delete_vector_contents<indri::net::NetworkStream*>( _streams );
  _streams.clear();
  indri::utility::delete_vector_contents<indri::collection::Repository*>( _repositories );
  _repositories.clear();
}

std::vector<std::string> indri::api::QueryEnvironment::documentMetadata( 
	const std::vector<DOCID_T>& documentIDs, const std::string& attributeName ) {
  std::vector< std::vector<DOCID_T> > docIDLists;
  docIDLists.resize( _servers.size() );
  std::vector< std::vector<DOCID_T> > docIDPositions;
  docIDPositions.resize( _servers.size() );
  std::vector< std::string > results;
  results.resize( documentIDs.size() );

  // split document numbers into lists for each query server
  qenv_scatter_document_ids( documentIDs, docIDLists, docIDPositions, (int)_servers.size() );

  indri::utility::greedy_vector<indri::server::QueryServerMetadataResponse*> responses;

  // send out requests for execution
  for( size_t i=0; i<docIDLists.size(); i++ ) {
    indri::server::QueryServerMetadataResponse* response = 0;
    
    if( docIDLists[i].size() )
      response = _servers[i]->documentMetadata( docIDLists[i], attributeName );
    
    responses.push_back(response);
  }

  // fold the results back into one master list (this method will delete the responses)
  qenv_gather_document_results( docIDLists, docIDPositions, responses, results );

  return results;
}

std::vector<std::string> indri::api::QueryEnvironment::documentMetadata( 
	const std::vector<indri::api::ScoredExtentResult>& results, const std::string& attributeName ) {
  // copy into an int vector
  std::vector<DOCID_T> documentIDs;
  documentIDs.reserve(results.size());

  for( size_t i=0; i<results.size(); i++ ) {
    documentIDs.push_back( results[i].document );
  }

  return documentMetadata( documentIDs, attributeName );
}

//
// _scoredQuery
//

void indri::api::QueryEnvironment::_scoredQuery( 
  indri::infnet::InferenceNetwork::MAllResults& results, 
  std::string& accumulatorName, 
  int resultsRequested ) {

  accumulatorName = "";

  // For each server, make a FilterNode and an AccumulatorNode, then run the query.
  // The filter node makes sure that we only score the documents that are interesting
  // so we don't waste too much time here.  
  //std::vector<indri::lang::Node*> nodes;
  std::vector<indri::server::QueryServerResponse*> queryResponses;

  for( size_t i=0; i<_servers.size(); i++ ) {
/*    indri::lang::ScoredExtentNode* scoredRoot = dynamic_cast<indri::lang::ScoredExtentNode*>(queryRoot);
    indri::lang::ScoreAccumulatorNode* accumulatorNode = 0;
    
    accumulatorNode = new indri::lang::ScoreAccumulatorNode( scoredRoot );
    nodes.push_back( accumulatorNode );

    // name all the nodes the same thing
    if( accumulatorNode ) {
      if( accumulatorName.length() > 0 ) {
        accumulatorNode->setNodeName( accumulatorName );
      } else {
        accumulatorName = accumulatorNode->nodeName();
      }
    }

    std::vector<indri::lang::Node*> root;
    root.push_back( accumulatorNode );*/

    // don't optimize these queries, otherwise we won't be able to distinguish some annotations from others
    //indri::server::QueryServerResponse* response = _servers[i]->runQuery( _queryDict, resultsRequested, true );
    //queryResponses.push_back(response);
  }

  // now, gather up all the responses, merge them into some kind of output structure, and return them
  _mergeQueryResults( results, queryResponses );
}

std::vector<indri::api::ScoredExtentResult> indri::api::QueryEnvironment::runQuery( 
	const std::string& query, int resultsRequested, const std::string &queryType ) {
  indri::infnet::InferenceNetwork::MAllResults results;
  std::vector<indri::api::ScoredExtentResult> queryResult = _runQuery( results, query, resultsRequested, queryType );
  return queryResult;
}
