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
// Repository
//
// 21 May 2004 -- tds
//

#include "indri/Repository.hpp"
#include "indri/CompressedCollection.hpp"
#include "indri/Path.hpp"
#include "indri/PorterStemmerTransformation.hpp"
#include "indri/StopperTransformation.hpp"
#include "indri/Parameters.hpp"
#include "indri/StemmerFactory.hpp"
#include "indri/NormalizationTransformation.hpp"
#include "indri/UTF8CaseNormalizationTransformation.hpp"
#include "lemur/Exception.hpp"
#include "indri/Thread.hpp"
#include "indri/DiskIndex.hpp"
#include "indri/ScopedLock.hpp"
#include "indri/RepositoryLoadThread.hpp"
#include "indri/RepositoryMaintenanceThread.hpp"
#include "indri/IndriTimer.hpp"
#include "indri/DirectoryIterator.hpp" 

#include <math.h>
#include <string>
#include <algorithm>

const static int defaultMemory = 100*1024*1024;

//
// _buildChain
//

void indri::collection::Repository::_buildChain( indri::api::Parameters& parameters, indri::api::Parameters* options ) {
  // Extract url from metadata before case normalizing.
  // this could be parameterized.

  bool dontNormalize = parameters.exists( "normalize" ) && ( false == (bool) parameters["normalize"] );

  if( dontNormalize == false ) {
    _transformations.push_back( new indri::parse::NormalizationTransformation() );
    _transformations.push_back( new indri::parse::UTF8CaseNormalizationTransformation() );
  }

  if( _parameters.exists("stopper.word") ) {
    indri::api::Parameters stop = _parameters["stopper.word"];
    _transformations.push_back( new indri::parse::StopperTransformation( stop ) );
  }
  // the transient chain stopwords need to precede the stemmer.
  if (options) {
    if( options->exists("stopper.word") ) {
      indri::api::Parameters stop = (*options)["stopper.word"];
      _transformations.push_back( new indri::parse::StopperTransformation( stop ) );
    }
  }

  if( _parameters.exists("stemmer.name") ) {
    std::string stemmerName = std::string(_parameters["stemmer.name"]);
    indri::api::Parameters stemmerParams = _parameters["stemmer"];
    _transformations.push_back( indri::parse::StemmerFactory::get( stemmerName, stemmerParams ) );
  }
}

//
// _copyParameters
//

void indri::collection::Repository::_copyParameters( indri::api::Parameters& options ) {
  if( options.exists( "normalize" ) ) {
    _parameters.set( "normalize", (std::string) options["normalize"] );
  }
  if( options.exists( "injectURL" ) ) {
    _parameters.set( "injectURL", (std::string) options["injectURL"] );
  }

  if( options.exists("stopper") ) {
    _parameters.set( "stopper", "" );
    _parameters["stopper"] = options["stopper"];
  }

  if( options.exists("stemmer") ) {
    _parameters.set( "stemmer", "" );
    _parameters["stemmer"] = options["stemmer"];
  }

}

//
// _remove
//
// In the future, this will remove a directory asynchronously,
// and will be cancellable.
//

void indri::collection::Repository::_remove( const std::string& indexPath ) {
  indri::file::Path::remove( indexPath );
}

//
// _openIndexes
//

void indri::collection::Repository::_openIndexes( indri::api::Parameters& params, const std::string& parentPath ) {
  try {
    indri::api::Parameters container = params["indexes"];

    _active = new index_vector;
    _states.push_back( _active );
    _indexCount = params.get( "indexCount", 0 );

    if( container.exists( "index" ) ) {
      indri::api::Parameters indexes = container["index"];

      for( size_t i=0; i<indexes.size(); i++ ) {
        indri::api::Parameters indexSpec = indexes[i];
        indri::index::DiskIndex* diskIndex = new indri::index::DiskIndex();
        std::string indexName = (std::string) indexSpec;

        diskIndex->open( parentPath, indexName );
        _active->push_back( diskIndex );
      }
    }
  } catch( lemur::api::Exception& e ) {
    LEMUR_RETHROW( e, "_openIndexes: Couldn't open DiskIndexes because:" );
  }
}

//
// countQuery
//
// Counts each document add--useful for load average computation.
//

void indri::collection::Repository::countQuery() {
  indri::atomic::increment( _queryLoad[0] );
}

//
// _countDocumentAdd
//
// Counts each document add--useful for load average computation.
//

void indri::collection::Repository::_countDocumentAdd() {
  indri::atomic::increment( _documentLoad[0] );
}

//
// _incrementLoad
//
// Called four times a minute by a timer thread to update the load average
//

void indri::collection::Repository::_incrementLoad() {
  memmove( (void*) &_documentLoad[1], (void*) &_documentLoad[0], (sizeof _documentLoad[0]) * (LOAD_MINUTES * LOAD_MINUTE_FRACTION - 1) );
  memmove( (void*) &_queryLoad[1], (void*) &_queryLoad[0], (sizeof _queryLoad[0]) * (LOAD_MINUTES * LOAD_MINUTE_FRACTION - 1) );

  _documentLoad[0] = 0;
  _queryLoad[0] = 0;
}

//
// _computeLoad
//

indri::collection::Repository::Load indri::collection::Repository::_computeLoad( indri::atomic::value_type* loadArray ) {
  Load load;

  load.one = load.five = load.fifteen = 0;

  for( int i=0; i<LOAD_MINUTE_FRACTION; i++ ) {
    load.one += loadArray[i];
  }

  for( int i=0; i<5*LOAD_MINUTE_FRACTION; i++ ) { 
    load.five += loadArray[i];
  }
  load.five /= 5.;

  for( int i=0; i<15*LOAD_MINUTE_FRACTION; i++ ) {
    load.fifteen += loadArray[i];
  }
  load.fifteen /= 15.;

  return load;
}

//
// queryLoad
//

indri::collection::Repository::Load indri::collection::Repository::queryLoad() {
  return _computeLoad( _queryLoad );
}

//
// documentLoad
//

indri::collection::Repository::Load indri::collection::Repository::documentLoad() {
  return _computeLoad( _documentLoad );
}

//
// openRead
//

void indri::collection::Repository::openRead( const std::string& path, indri::api::Parameters* options ) {
  try {
    _path = path;
    _readOnly = true;

    _memory = defaultMemory;
    if( options )
      _memory = options->get( "memory", _memory );

    float queryProportion = 1;
    if( options )
      queryProportion = static_cast<float>(options->get( "queryProportion", queryProportion ));

    _parameters.loadFile( indri::file::Path::combine( path, "manifest" ) );

    _buildChain( _parameters, options );

    std::string indexPath = indri::file::Path::combine( path, "index" );
    std::string collectionPath = indri::file::Path::combine( path, "collection" );
    std::string indexName = indri::file::Path::combine( indexPath, "index" );
    std::string deletedName = indri::file::Path::combine( path, "deleted" );

    _openIndexes( _parameters, indexPath );

    _collection = new CompressedCollection();
    _collection->openRead( collectionPath );
    _deletedList.read( deletedName );

    _startThreads();
  } catch( lemur::api::Exception& e ) {
    LEMUR_RETHROW( e, "Couldn't open a repository in read-only mode at '" + path + "' because:" );
  } catch( ... ) {
    LEMUR_THROW( LEMUR_RUNTIME_ERROR, "Something unexpected happened while trying to create '" + path + "'" );
  }
}

//
// open
//

void indri::collection::Repository::open( const std::string& path, indri::api::Parameters* options ) {

}

//
// exists
//

bool indri::collection::Repository::exists( const std::string& path ) {
  std::string manifestPath = indri::file::Path::combine( path, "manifest" );
  return indri::file::Path::exists( manifestPath );
}

//
// deleteDocument
//

void indri::collection::Repository::deleteDocument( int documentID ) {
  _deletedList.markDeleted( documentID );
}

//
// _swapState
//
// Make a new state object, swap in the new index for the old one
//

void indri::collection::Repository::_swapState( std::vector<indri::index::Index*>& oldIndexes, indri::index::Index* newIndex ) {
  indri::thread::ScopedLock lock( _stateLock );

  index_state oldState = _active;
  _active = new index_vector;

  size_t i;
  // copy all states up to oldIndexes
  for( i=0; i<oldState->size() && (*oldState)[i] != oldIndexes[0]; i++ ) {
    _active->push_back( (*oldState)[i] );
  }

  size_t firstMatch = i;

  // verify (in debug builds) that all the indexes match up like they should
  for( ; i<oldState->size() && (i-firstMatch) < oldIndexes.size(); i++ ) {
    assert( (*oldState)[i] == oldIndexes[i-firstMatch] );
  }

  // add the new index
  _active->push_back( newIndex );

  // copy all trailing indexes
  for( ; i<oldState->size(); i++ ) {
    _active->push_back( (*oldState)[i] );
  }

  _states.push_back( _active );
}

//
// _removeStates
//
// Remove a certain number of states from the _states vector
//

void indri::collection::Repository::_removeStates( std::vector<index_state>& toRemove ) {
  for( size_t i=0; i<toRemove.size(); i++ ) {
    std::vector<index_state>::iterator iter;

    for( iter = _states.begin(); iter != _states.end(); iter++ ) {
      if( (*iter) == toRemove[i] ) {
        _states.erase( iter );
        break;
      }
    }
  }
}

//
// _stateContains
//
// Returns true if the state contains any one of the given indexes
//

bool indri::collection::Repository::_stateContains( index_state& state, std::vector<indri::index::Index*>& indexes ) {
  // for every index in this state
  for( size_t j=0; j<state->size(); j++ ) {
    // does it match one of our indexes?
    for( size_t k=0; k<indexes.size(); k++ ) {
      if( (*state)[j] == indexes[k] ) {
        return true;
      }
    }
  }

  // no match
  return false;
}

//
// _statesContaining
//
// Find all states that contain any of these indexes
//

std::vector<indri::collection::Repository::index_state> indri::collection::Repository::_statesContaining( std::vector<indri::index::Index*>& indexes ) {
  indri::thread::ScopedLock lock( _stateLock );
  std::vector<index_state> result;

  // for every current state
  for( size_t i=0; i<_states.size(); i++ ) {
    index_state& state = _states[i];

    if( _stateContains( state, indexes ) )
      result.push_back( state );
  }
  
  return result;
}

//
// _closeIndexes
//

void indri::collection::Repository::_closeIndexes() {
  // we assume we don't need locks, because the one running
  // the repository has stopped all queries and document adds, etc.

  // drops all states except active to reference count 0, so they get deleted
  _states.clear();

  for( size_t i=0; i<_active->size(); i++ ) {
    (*_active)[i]->close();
    delete (*_active)[i];
  }

  // deletes the active state
  _active = 0;
}

void print_index_state( std::vector<indri::collection::Repository::index_state>& states ) {
  for( size_t i=0; i<states.size(); i++ ) {
    for( size_t j=0; j<states[i]->size(); j++ ) {
      std::cout << i << " " << (*states[i])[j] << std::endl;
    }
  }
}

//
// processTerm
//

std::string indri::collection::Repository::processTerm( const std::string& term ) {
  indri::api::ParsedDocument original;
  indri::api::ParsedDocument* document;
  std::string result;
  char termBuffer[lemur::file::Keyfile::MAX_KEY_LENGTH];
  if( term.length() >= lemur::file::Keyfile::MAX_KEY_LENGTH ) {
    return term;
  }
    //  assert( term.length() < sizeof termBuffer );
  strcpy( termBuffer, term.c_str() );

  original.text = termBuffer;
  original.textLength = strlen(termBuffer)+1;

  original.terms.push_back( termBuffer );
  document = &original;
  indri::thread::ScopedLock lock( _addLock );  
  for( size_t i=0; i<_transformations.size(); i++ ) {
    document = _transformations[i]->transform( document );    
  }
  
  if( document->terms[0] )
    result = document->terms[0];

  return result;
}

//
// collection
//

indri::collection::CompressedCollection* indri::collection::Repository::collection() {
  return _collection;
}

//
// deletedList
//

indri::index::DeletedDocumentList& indri::collection::Repository::deletedList() {
  return _deletedList;
}

//
// close
//

void indri::collection::Repository::close() {
  if( _collection ) {
    // TODO: make sure all the indexes get deleted
    std::string manifest = "manifest";
    std::string paramPath = indri::file::Path::combine( _path, manifest );
    std::string deletedPath = indri::file::Path::combine( _path, "deleted" );

    // have to stop threads after the write request,
    // so the indexes actually get written
    _stopThreads();

    _closeIndexes();

    delete _collection;
    _collection = 0;

    _parameters.clear(); // close/reopen will cause duplicated entries.
    indri::utility::delete_vector_contents( _transformations );
  }
}

//
// indexes
//

indri::collection::Repository::index_state indri::collection::Repository::indexes() {
  // calling this method implies that some query-related operation
  // is about to happen
  return _active;
}

//
// _startThreads
//

void indri::collection::Repository::_startThreads() {
  _maintenanceThread = 0;

  if( !_readOnly ) {
    _loadThread = new RepositoryLoadThread( *this, _memory );
    _loadThread->start();
  } else {
    _loadThread = 0;
  }
}

//
// _stopThreads
//

void indri::collection::Repository::_stopThreads() {
  if( !_loadThread && !_maintenanceThread )
    return;

  if( _maintenanceThread )
    _maintenanceThread->signal();
  if( _loadThread )
    _loadThread->signal();

  if( _loadThread ) {
    _loadThread->join();
    delete _loadThread;
    _loadThread = 0;
  }

  if( _maintenanceThread ) {
    _maintenanceThread->join();
    delete _maintenanceThread;
    _maintenanceThread = 0;
  }
}

//
// _setThrashing
//

void indri::collection::Repository::_setThrashing( bool flag ) {
  _thrashing = flag;
  
  if( _thrashing ) {
    _lastThrashTime = indri::utility::IndriTimer::currentTime();
  }
}

//
// _timeSinceThrashing
//

UINT64 indri::collection::Repository::_timeSinceThrashing() {
  return indri::utility::IndriTimer::currentTime() - _lastThrashTime;
}

//
// _stemmerName
//

std::string indri::collection::Repository::_stemmerName( indri::api::Parameters& parameters ) {
  return parameters.get( "stemmer.name", "" );
}  
