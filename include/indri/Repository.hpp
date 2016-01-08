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

#ifndef INDRI_REPOSITORY_HPP
#define INDRI_REPOSITORY_HPP

#include <iterator>

#include "indri/Parameters.hpp"
#include "indri/Transformation.hpp"
#include "indri/DiskIndex.hpp"
#include "indri/ref_ptr.hpp"
#include "indri/DeletedDocumentList.hpp"
#include <string>
// 512 -- syslimit can be 1024
#define MERGE_FILE_LIMIT 768 
namespace indri
{
  /// document manager and ancillary collection components.
  namespace collection
  {
    
    /*! Encapsulates document manager, index, and field indexes. Provides access 
     *  to collection for both IndexEnvironment and QueryEnvironment.
     */
    class Repository {
    public:
      struct Load {
        float one;
        float five;
        float fifteen;
      };

      typedef std::vector<indri::index::Index*> index_vector;
      typedef indri::atomic::ref_ptr<index_vector> index_state;

    private:
      friend class RepositoryMaintenanceThread;
      friend class RepositoryLoadThread;

      class RepositoryMaintenanceThread* _maintenanceThread;
      class RepositoryLoadThread* _loadThread;

      indri::thread::Mutex _stateLock; /// protects against state changes
      std::vector<index_state> _states;
      index_state _active;
      int _indexCount;

      // running flags
      volatile bool _maintenanceRunning;
      volatile bool _loadThreadRunning;

      indri::thread::Mutex _addLock; /// protects addDocument

      class CompressedCollection* _collection;
      indri::index::DeletedDocumentList _deletedList;

      indri::api::Parameters _parameters;
      std::vector<indri::parse::Transformation*> _transformations;

      std::string _path;
      bool _readOnly;

      INT64 _memory;

      UINT64 _lastThrashTime;
      volatile bool _thrashing;

      enum { LOAD_MINUTES = 15, LOAD_MINUTE_FRACTION = 12 };

      indri::atomic::value_type _queryLoad[ LOAD_MINUTES * LOAD_MINUTE_FRACTION ];
      indri::atomic::value_type _documentLoad[ LOAD_MINUTES * LOAD_MINUTE_FRACTION ];

      static std::string _stemmerName( indri::api::Parameters& parameters );
      static void _cleanAndCreateDirectory( const std::string& path );

      void _incrementLoad();
      void _countDocumentAdd();
      Load _computeLoad( indri::atomic::value_type* loadArray );

      void _buildChain( indri::api::Parameters& parameters,
                        indri::api::Parameters *options );

      void _copyParameters( indri::api::Parameters& options );

      void _removeStates( std::vector<index_state>& toRemove );
      void _remove( const std::string& path );

      void _openIndexes( indri::api::Parameters& params, const std::string& parentPath );
      std::vector<index_state> _statesContaining( std::vector<indri::index::Index*>& indexes );
      bool _stateContains( index_state& state, std::vector<indri::index::Index*>& indexes );
      void _swapState( std::vector<indri::index::Index*>& oldIndexes, indri::index::Index* newIndex );
      void _closeIndexes();

      void _startThreads();
      void _stopThreads();

      void _setThrashing( bool flag );
      UINT64 _timeSinceThrashing();

    public:
      Repository() {
        _collection = 0;
        _readOnly = false;
        _lastThrashTime = 0;
        _thrashing = false;
        memset( (void*) _documentLoad, 0, sizeof(indri::atomic::value_type)*LOAD_MINUTES*LOAD_MINUTE_FRACTION );
        memset( (void*) _queryLoad, 0, sizeof(indri::atomic::value_type)*LOAD_MINUTES*LOAD_MINUTE_FRACTION );
      }

      ~Repository() {
        close();
      }
      /// delete a document from the repository
      /// @param documentID the internal ID of the document to delete
      void deleteDocument( int documentID );
      /// Process, possibly transforming, the given term
      /// @param term the term to process
      /// @return the processed term
      std::string processTerm( const std::string& term );
      /// @return the compressed document collection
      class CompressedCollection* collection();
      /// Open an existing repository.
      /// @param path the directory to open the repository from
      /// @param options additional parameters
      void open( const std::string& path, indri::api::Parameters* options = 0 );
      /// Open an existing repository in read only mode.
      /// @param path the directory to open the repository from
      /// @param options additional parameters
      void openRead( const std::string& path, indri::api::Parameters* options = 0 );
      /// @return true if a valid Indri Repository resides in the named path
      /// false otherwise.
      /// @param path the directory to open the repository from
      static bool exists( const std::string& path );
      /// Close the repository
      void close();

      /// Compact the repository by removing all information about
      /// deleted documents from disk.
      void compact();

      /// Indexes in this repository
      index_state indexes();

      /// Notify the repository that a query has happened
      void countQuery();

      /// List of deleted documents in this repository
      indri::index::DeletedDocumentList& deletedList();

      /// Returns the average number of documents added each minute in the last 1, 5 and 15 minutes
      Load queryLoad();

      /// Returns the average number of documents added each minute in the last 1, 5 and 15 minutes
      Load documentLoad();
    };
  }
}

#endif // INDRI_REPOSITORY_HPP

