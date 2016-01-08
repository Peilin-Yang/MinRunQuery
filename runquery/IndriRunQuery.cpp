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
// runquery
//
// 24 February 2004 -- tds
//

#include <time.h>
#include "indri/QueryEnvironment.hpp"
#include "indri/delete_range.hpp"

#include "indri/Parameters.hpp"

#include "indri/UtilityThread.hpp"
#include "indri/ScopedLock.hpp"
#include "indri/delete_range.hpp"

#include <queue>

static bool copy_parameters_to_string_vector( std::vector<std::string>& vec, indri::api::Parameters p, const std::string& parameterName ) {
  if( !p.exists(parameterName) )
    return false;

  indri::api::Parameters slice = p[parameterName];

  for( size_t i=0; i<slice.size(); i++ ) {
    vec.push_back( slice[i] );
  }

  return true;
}

struct query_t {
  struct greater {
    bool operator() ( query_t* one, query_t* two ) {
      return one->index > two->index;
    }
  };

  query_t( int _index, std::string _number, const std::string& _text, const std::string &queryType) :
    index( _index ),
    number( _number ),
    text( _text ), qType(queryType)
  {
  }

  query_t( int _index, std::string _number, const std::string& _text ) :
    index( _index ),
    number( _number ),
    text( _text )
  {
  }

  std::string number;
  int index;
  std::string text;
  std::string qType;
};

class QueryThread : public indri::thread::UtilityThread {
private:
  indri::thread::Lockable& _queueLock;
  indri::thread::ConditionVariable& _queueEvent;
  std::queue< query_t* >& _queries;
  std::priority_queue< query_t*, std::vector< query_t* >, query_t::greater >& _output;

  indri::api::QueryEnvironment _environment;
  indri::api::Parameters& _parameters;
  int _requested;
  std::string _runID;

  std::vector<indri::api::ScoredExtentResult> _results;

  // Runs the query, expanding it if necessary.  Will print output as well if verbose is on.
  void _runQuery( std::stringstream& output, const std::string& query,
                  const std::string &queryType) {
    try {
      _results = _environment.runQuery( query, _requested, queryType );
    }
    catch( lemur::api::Exception& e )
    {
      _results.clear();
      LEMUR_RETHROW(e, "QueryThread::_runQuery Exception");
    }
  }

  void _printResultRegion( std::stringstream& output, std::string queryIndex, int start, int end  ) {
    std::vector<std::string> documentNames;
    std::vector<indri::api::ParsedDocument*> documents;

    std::vector<indri::api::ScoredExtentResult> resultSubset;

    resultSubset.assign( _results.begin() + start, _results.begin() + end );

    documentNames = _environment.documentMetadata( resultSubset, "docno" );

    std::vector<std::string> pathNames;

    // Print results
    for( size_t i=0; i < resultSubset.size(); i++ ) {
      int rank = start+i+1;
      std::string queryNumber = queryIndex;

      // TREC formatted output: queryNumber, Q0, documentName, rank, score, runID
      output << queryNumber << " "
              << "Q0 "
              << documentNames[i] << " "
              << rank << " "
              << resultSubset[ i ].score << " "
              << _runID << std::endl;

      if( documents.size() )
        delete documents[i];
    }
  }

  void _printResults( std::stringstream& output, std::string queryNumber ) {
    for( size_t start = 0; start < _results.size(); start += 50 ) {
      size_t end = std::min<size_t>( start + 50, _results.size() );
      _printResultRegion( output, queryNumber, start, end );
    }
  }


public:
  QueryThread( std::queue< query_t* >& queries,
               std::priority_queue< query_t*, std::vector< query_t* >, query_t::greater >& output,
               indri::thread::Lockable& queueLock,
               indri::thread::ConditionVariable& queueEvent,
               indri::api::Parameters& params ) :
    _queries(queries),
    _output(output),
    _queueLock(queueLock),
    _queueEvent(queueEvent),
    _parameters(params)
  {
  }

  ~QueryThread() {
  }

  UINT64 initialize() {
    try {        
    _environment.setSingleBackgroundModel( _parameters.get("singleBackgroundModel", false) );

    std::vector<std::string> stopwords;
    if( copy_parameters_to_string_vector( stopwords, _parameters, "stopper.word" ) )
      _environment.setStopwords(stopwords);

    std::vector<std::string> smoothingRules;
    if( copy_parameters_to_string_vector( smoothingRules, _parameters, "rule" ) )
      _environment.setScoringRules( smoothingRules );

   if( _parameters.exists( "index" ) ) {
      indri::api::Parameters indexes = _parameters["index"];

      for( size_t i=0; i < indexes.size(); i++ ) {
        _environment.addIndex( std::string(indexes[i]) );
      }
    }

    if( _parameters.exists( "server" ) ) {
      indri::api::Parameters servers = _parameters["server"];

      for( size_t i=0; i < servers.size(); i++ ) {
        _environment.addServer( std::string(servers[i]) );
      }
    }

    _requested = _parameters.get( "count", 1000 );
    _runID = _parameters.get( "runID", "indri" );

    } catch ( lemur::api::Exception& e ) {      
      while( _queries.size() ) {
        query_t *query = _queries.front();
        _queries.pop();
        _output.push( new query_t( query->index, query->number, "query: " + query->number + " QueryThread::_initialize exception\n" ) );
        _queueEvent.notifyAll();
        LEMUR_RETHROW(e, "QueryThread::_initialize");
      }
    }
    return 0;
  }

  void deinitialize() {
    _environment.close();
  }

  bool hasWork() {
    indri::thread::ScopedLock sl( &_queueLock );
    return _queries.size() > 0;
  }

  UINT64 work() {
    query_t* query;
    std::stringstream output;

    // pop a query off the queue
    {
      indri::thread::ScopedLock sl( &_queueLock );
      if( _queries.size() ) {
        query = _queries.front();
        _queries.pop();
      } else {
        return 0;
      }
    }

    // run the query
    try {
      _runQuery( output, query->text, query->qType );
    } catch( lemur::api::Exception& e ) {
      output << "# EXCEPTION in query " << query->number << ": " << e.what() << std::endl;
    }

    // print the results to the output stream
    _printResults( output, query->number );

    // push that data into an output queue...?
    {
      indri::thread::ScopedLock sl( &_queueLock );
      _output.push( new query_t( query->index, query->number, output.str() ) );
      _queueEvent.notifyAll();
    }

    delete query;
    return 0;
  }
};

void push_queue( std::queue< query_t* >& q, indri::api::Parameters& queries ) {

  for( size_t i=0; i<queries.size(); i++ ) {
    std::string queryNumber;
    std::string queryText;
    std::string queryType = "indri";
    if( queries[i].exists( "type" ) )
      queryType = (std::string) queries[i]["type"];
    if (queries[i].exists("text"))
      queryText = (std::string) queries[i]["text"];
    if( queries[i].exists( "number" ) ) {
      queryNumber = (std::string) queries[i]["number"];
    }
    if (queryText.size() == 0)
      queryText = (std::string) queries[i];

    q.push( new query_t( i, queryNumber, queryText, queryType ) );
  }
}

int main(int argc, char * argv[]) {
  try {
    indri::api::Parameters& param = indri::api::Parameters::instance();
    param.loadCommandLine( argc, argv );

    if( param.get( "version", 0 ) ) {
      std::cout << INDRI_DISTRIBUTION << std::endl;
    }

    if( !param.exists( "query" ) )
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR, "Must specify at least one query." );

    if( !param.exists("index") && !param.exists("server") )
      LEMUR_THROW( LEMUR_MISSING_PARAMETER_ERROR, "Must specify a server or index to query against." );

    int threadCount = param.get( "threads", 1 );
    std::queue< query_t* > queries;
    std::priority_queue< query_t*, std::vector< query_t* >, query_t::greater > output;
    std::vector< QueryThread* > threads;
    indri::thread::Mutex queueLock;
    indri::thread::ConditionVariable queueEvent;

    // push all queries onto a queue
    indri::api::Parameters parameterQueries = param[ "query" ];
    push_queue( queries, parameterQueries );
    int queryCount = (int)queries.size();

    // launch threads
    for( int i=0; i<threadCount; i++ ) {
      threads.push_back( new QueryThread( queries, output, queueLock, queueEvent, param ) );
      threads.back()->start();
    }

    int query = 0;

    // acquire the lock.
    queueLock.lock();

    // process output as it appears on the queue
    while( query < queryCount ) {
      query_t* result = NULL;

      // wait for something to happen
      queueEvent.wait( queueLock );

      while( output.size() && output.top()->index == query ) {
        result = output.top();
        output.pop();

        queueLock.unlock();

        std::cout << result->text;
        delete result;
        query++;

        queueLock.lock();
      }
    }
    queueLock.unlock();

    // join all the threads
    for( size_t i=0; i<threads.size(); i++ )
      threads[i]->join();

    // we've seen all the query output now, so we can quit
    indri::utility::delete_vector_contents( threads );
  } catch( lemur::api::Exception& e ) {
    LEMUR_ABORT(e);
  } catch( ... ) {
    std::cout << "Caught unhandled exception" << std::endl;
    return -1;
  }

  return 0;
}

