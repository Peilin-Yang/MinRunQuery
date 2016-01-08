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
// TermList
//
// 23 November 2004 -- tds
//

#ifndef INDRI_TERMLIST_HPP
#define INDRI_TERMLIST_HPP

#include "indri/greedy_vector"
#include "lemur/RVLCompress.hpp"
#include "indri/Buffer.hpp"
#include "indri/RVLCompressStream.hpp"
#include "indri/RVLDecompressStream.hpp"
#include "lemur/IndexTypes.hpp"

namespace indri {
  namespace index {
    class TermList {
    private:
      indri::utility::greedy_vector<lemur::api::TERMID_T> _terms;

    public:
      void clear() {
        _terms.clear();
      }
      
      void addTerm( const lemur::api::TERMID_T termID ) {
        _terms.push_back( termID );
      }
      
      indri::utility::greedy_vector<lemur::api::TERMID_T>& terms() {
        return _terms;
      }
      
      const indri::utility::greedy_vector<lemur::api::TERMID_T>& terms() const {
        return _terms;
      }
      
      void read( const char* buffer, int size ) {
        clear();
        indri::utility::RVLDecompressStream stream( buffer, size );
        
        int termCount;
        
        stream >> termCount;
        
        for( int i=0; i<termCount; i++ ) {
          lemur::api::TERMID_T termID;
          stream >> termID;

          assert( termID >= 0 );
          _terms.push_back( termID ); 
        }
      }
      
      void write( indri::utility::Buffer& buffer ) {
        // format:
        //   term count
        //   field count
        //   termID * termCount (compressed)
        //   ( fieldID, begin, end, number ) * fieldCount
        
        indri::utility::RVLCompressStream out( buffer );
        
        // write count of terms and fields in the document first
        int termCount = (int)_terms.size();
        out << termCount;
        
        // write out terms
        for( size_t i=0; i<_terms.size(); i++ ) {
          assert( _terms[i] >= 0 );
          out << _terms[i];
        }
      }
    };
  }
}

#endif // INDRI_TERMLIST_HPP
