/*==========================================================================
 * Copyright (c) 2003-2004 University of Massachusetts.  All Rights Reserved.
 *
 * Use of the Lemur Toolkit for Language Modeling and Information Retrieval
 * is subject to the terms of the software license set forth in the LICENSE
 * file included with this software, and also available at
 * http://www.lemurproject.org/license.html
 *
 *==========================================================================
 */


//
// CompressedCollection
//
// 12 May 2004 -- tds
//

#include "indri/CompressedCollection.hpp"
#include "zlib.h"
#include "lemur/string-set.h"
#include "indri/XMLNode.hpp"
#include "lemur/Exception.hpp"
#include "lemur/RVLCompress.hpp"
#include "indri/RVLDecompressStream.hpp"
#include "indri/RVLCompressStream.hpp"
#include "indri/Buffer.hpp"
#include "indri/Path.hpp"
#include "indri/Parameters.hpp"
#include "indri/File.hpp"
#include "indri/ScopedLock.hpp"
#include <algorithm>

const int INPUT_BUFFER_SIZE = 1024;
const int OUTPUT_BUFFER_SIZE = 128*1024;
const char POSITIONS_KEY[] = "#POSITIONS#";
const char TEXT_KEY[] = "#TEXT#";
const char CONTENT_KEY[] = "#CONTENT#";
const char CONTENTLENGTH_KEY[] = "#CONTENTLENGTH#";

//
// zlib_alloc
//

static void* zlib_alloc( void* opaque, uInt items, uInt size ) {
  return malloc( items*size );
}

//
// zlib_free
//

static void zlib_free( void* opaque, void* address ) {
  free( address );
}

//
// zlib_read_document
//

static void zlib_read_document( z_stream_s& stream, indri::file::File& infile, UINT64 offset, indri::utility::Buffer& outputBuffer ) {
  // read in data from the file until the stream ends
  // split up the data as necessary
  // decompress positional info

  // read some data
  char inputBuffer[INPUT_BUFFER_SIZE];
  outputBuffer.grow( INPUT_BUFFER_SIZE );
  outputBuffer.write( sizeof(indri::api::ParsedDocument) );

  stream.avail_in = 0;
  stream.avail_out = 0;
  
  while(true) {
    if( !stream.avail_in ) {
      UINT64 readSize = infile.read( inputBuffer, offset, sizeof inputBuffer );
      offset += readSize; 
      
      stream.avail_in = readSize;
      stream.next_in = (Bytef*) inputBuffer;
    }

    stream.avail_out = outputBuffer.size() - outputBuffer.position();
    stream.next_out = (Bytef*) outputBuffer.write( outputBuffer.size() - outputBuffer.position() );

    int result = inflate( &stream, Z_NO_FLUSH );
    outputBuffer.unwrite( stream.avail_out );

    if( result == Z_STREAM_END ) {
      result = inflate( &stream, Z_FINISH );
      
      if( result < 0 )
        LEMUR_THROW( result, "Something bad happened while trying to finish decompressing a document." );

      inflateEnd( &stream );
      break;
    }

    if( result < 0 ) {
      LEMUR_THROW( result, "Something bad happened while trying to decompress a document." );
    }

    if( stream.avail_out == 0 ) {
      outputBuffer.grow();
    }
  }
}

//
// copy_quad
//

static int copy_quad( char* buffer ) {
  unsigned char firstByte = buffer[0];
  unsigned char secondByte = buffer[1];
  unsigned char thirdByte = buffer[2];
  unsigned char fourthByte = buffer[3];

  int result;

  ( (char*) &result )[0] = firstByte;
  ( (char*) &result )[1] = secondByte;
  ( (char*) &result )[2] = thirdByte;
  ( (char*) &result )[3] = fourthByte;

  return result;
}

//
// _readPositions
//

void indri::collection::CompressedCollection::_readPositions( indri::api::ParsedDocument* document, const void* positionData, int positionDataLength ) {
  indri::utility::RVLDecompressStream decompress( (const char*) positionData, positionDataLength );
  int last = 0;

  while( !decompress.done() ) {
    indri::parse::TermExtent extent;

    decompress >> extent.begin
               >> extent.end;

    extent.begin += last;
    extent.end += extent.begin;
    last = extent.end;

    document->positions.push_back( extent );
  }
}

//
// CompressedCollection
//

indri::collection::CompressedCollection::CompressedCollection() {
  _stream = new z_stream_s;
  _stream->zalloc = zlib_alloc;
  _stream->zfree = zlib_free;
  _stream->next_out = 0;
  _stream->avail_out = 0;

  deflateInit( _stream, Z_BEST_SPEED );

  _strings = string_set_create();
  _output = 0;
}

//
// ~CompressedCollection
//

indri::collection::CompressedCollection::~CompressedCollection() {
  close();

  delete _output;
  deflateEnd( _stream );
  delete _stream;
  string_set_delete( _strings );
}

void indri::collection::CompressedCollection::reopen( const std::string& fileName ) {
  indri::thread::ScopedLock l( _lock );
  close();
  open(fileName);
}

//
// open
//

void indri::collection::CompressedCollection::open( const std::string& fileName ) {
  std::string lookupName = indri::file::Path::combine( fileName, "lookup" );
  std::string storageName = indri::file::Path::combine( fileName, "storage" );
  std::string manifestName = indri::file::Path::combine( fileName, "manifest" );

  indri::api::Parameters manifest;
  manifest.loadFile( manifestName );

  _basePath = fileName;
  _storage.open( storageName );
  _lookup.open( lookupName );
  _output = new indri::file::SequentialWriteBuffer( _storage, 1024*1024 );

  _storeDocs = manifest.get( "storeDocs", true );
  if( manifest.exists("forward.field") ) {
    indri::api::Parameters forward = manifest["forward.field"];

    for( size_t i=0; i<forward.size(); i++ ) {
      std::stringstream metalookupName;
      metalookupName << "forwardLookup" << (int)i;

      std::string metalookupPath = indri::file::Path::combine( fileName, metalookupName.str() );
      lemur::file::Keyfile* metalookup = new lemur::file::Keyfile;
      metalookup->open( metalookupPath );

      std::string fieldName = forward[i];
      const char* key = string_set_add( fieldName.c_str(), _strings );
      _forwardLookups.insert( key, metalookup );
    }
  }

  indri::api::Parameters reverse = manifest["reverse"];

  if( manifest.exists("reverse.field") ) {
    indri::api::Parameters reverse = manifest["reverse.field"];

    for( size_t i=0; i<reverse.size(); i++ ) {
      std::stringstream metalookupName;
      metalookupName << "reverseLookup" << (int)i;

      std::string metalookupPath = indri::file::Path::combine( fileName, metalookupName.str() );
      lemur::file::Keyfile* metalookup = new lemur::file::Keyfile;
      metalookup->open( metalookupPath );

      std::string fieldName = reverse[i];
      const char* key = string_set_add( fieldName.c_str(), _strings );
      _reverseLookups.insert( key, metalookup );
    }
  }

}

//
// openRead
//

void indri::collection::CompressedCollection::openRead( const std::string& fileName ) {
  std::string lookupName = indri::file::Path::combine( fileName, "lookup" );
  std::string storageName = indri::file::Path::combine( fileName, "storage" );
  std::string manifestName = indri::file::Path::combine( fileName, "manifest" );

  indri::api::Parameters manifest;
  manifest.loadFile( manifestName );

  _basePath = fileName;
  _storage.openRead( storageName );
  _lookup.openRead( lookupName );

  if( manifest.exists("forward.field") ) {
    indri::api::Parameters forward = manifest["forward.field"];

    for( size_t i=0; i<forward.size(); i++ ) {
      std::stringstream metalookupName;
      metalookupName << "forwardLookup" << (int)i;

      std::string metalookupPath = indri::file::Path::combine( fileName, metalookupName.str() );
      lemur::file::Keyfile* metalookup = new lemur::file::Keyfile;
      metalookup->openRead( metalookupPath );

      std::string fieldName = forward[i];
      const char* key = string_set_add( fieldName.c_str(), _strings );
      _forwardLookups.insert( key, metalookup );
    }
  }

  if( manifest.exists("reverse.field") ) {
    indri::api::Parameters reverse = manifest["reverse.field"];

    for( size_t i=0; i<reverse.size(); i++ ) {
      std::stringstream metalookupName;
      metalookupName << "reverseLookup" << (int)i;

      std::string metalookupPath = indri::file::Path::combine( fileName, metalookupName.str() );
      lemur::file::Keyfile* metalookup = new lemur::file::Keyfile;
      metalookup->openRead( metalookupPath );

      std::string fieldName = reverse[i];
      const char* key = string_set_add( fieldName.c_str(), _strings );
      _reverseLookups.insert( key, metalookup );
    }
  }
}

//
// close
//

void indri::collection::CompressedCollection::close() {

}

//
// keys_to_vector
//

static std::vector<std::string> keys_to_vector( indri::utility::HashTable<const char*, lemur::file::Keyfile*>& table ) {
  std::vector<std::string> result;
  indri::utility::HashTable<const char*, lemur::file::Keyfile*>::iterator iter;

  for( iter = table.begin(); iter != table.end(); iter++ ) {
    result.push_back( *(iter->first) );
  }

  return result;
}

//
// retrieveMetadatum
//

std::string indri::collection::CompressedCollection::retrieveMetadatum( lemur::api::DOCID_T documentID, const std::string& attributeName ) {
  indri::thread::ScopedLock l( _lock );

  lemur::file::Keyfile** metalookup = _forwardLookups.find( attributeName.c_str() );
  std::string result;

  if( metalookup ) {
    char* resultBuffer = 0;
    int length = 0;
    bool success = (*metalookup)->get( documentID, &resultBuffer, length );

    if( success ) {
      // assuming result is of a string type
      result.assign( resultBuffer, length-1 );
    }

    delete[] resultBuffer;
  }

  return result;
}

//
// remove_deleted_entries
//

static void remove_deleted_entries( indri::utility::Buffer& value, indri::index::DeletedDocumentList& deletedList ) {
  int idCount = (int)value.position() / sizeof (lemur::api::DOCID_T);
  int startIDCount = idCount;
  for( int i = 0; i < idCount; ) {
    lemur::api::DOCID_T* position = &((lemur::api::DOCID_T*) value.front())[i];
    lemur::api::DOCID_T document = *position;

    if( deletedList.isDeleted( document ) ) {
      // remove this documentID by moving all remaining docIDs down
      ::memmove( position,
                 position + 1,
                 sizeof (lemur::api::DOCID_T) * (idCount - i - 1) );
      idCount--;
    } else {
      // move to the next documentID
      i++;
    }
  }

  if( startIDCount > idCount )
    value.unwrite( (startIDCount - idCount) * sizeof (lemur::api::DOCID_T) );
}

//
// keyfile_get
//

static bool keyfile_get( lemur::file::Keyfile& keyfile, int key, indri::utility::Buffer& value ) {
  bool result = false;
  int actualValueSize = (int)value.size();
  value.clear();

  try {
    result = keyfile.get( key, value.front(), actualValueSize, (int)value.size() );
  } catch( lemur::api::Exception& ) {
    int size = keyfile.getSize( key );
    if( size >= 0 ) {
      value.grow( size );
      actualValueSize = (int)value.size();
      keyfile.get( key, value.front(), actualValueSize, (int)value.size() );
      result = true;
    }
  }

  if( result )
    value.write( actualValueSize );
  return result;
}

//
// keyfile_get
//


static bool keyfile_get( lemur::file::Keyfile& keyfile, char* key, indri::utility::Buffer& value ) {
  bool result = false;
  int actualValueSize = (int)value.size();
  value.clear();

  try {
    result = keyfile.get( key, value.front(), actualValueSize, (int)value.size() );
  } catch( lemur::api::Exception& ) {
    int size = keyfile.getSize( key );
    if( size >= 0 ) {
      value.grow( size );
      actualValueSize = (int)value.size();
      keyfile.get( key, value.front(), actualValueSize, (int)value.size() );
      result = true;
    }
  }

  if( result )
    value.write( actualValueSize );
  return result;
}