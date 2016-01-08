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
// DagCopier
//
// 5 March 2004 -- tds
//

#ifndef INDRI_DAGCOPIER_HPP
#define INDRI_DAGCOPIER_HPP

#include <vector>
#include "indri/delete_range.hpp"
namespace indri 
{
  namespace lang
  {
    
    class DagCopier : public indri::lang::Copier {
    private:
      std::vector<indri::lang::IndexTerm*> _terms;
      std::vector<indri::lang::Node*> _newNodes;

      template<class T>
      T* _findReplacement( std::vector<T*>& replacements, T* candidate ) {
        T* replacement = 0;
    
        for( unsigned int i=0; i<replacements.size(); i++ ) {
          if( (*candidate) == (*replacements[i]) ) {
            replacement = replacements[i];
            break;
          }
        }

        if( replacement ) {
          delete candidate;
          candidate = replacement;
        } else {
          _newNodes.push_back( candidate );
          replacements.push_back( candidate );
        }

        return candidate;
      }

    public:
      ~DagCopier() {
        indri::utility::delete_vector_contents( _newNodes );
      }

      indri::lang::Node* defaultAfter( indri::lang::Node* oldNode, indri::lang::Node* newNode ) {
        _newNodes.push_back( newNode );
        return newNode;
      }

      indri::lang::Node* after( indri::lang::IndexTerm* indexTerm, indri::lang::IndexTerm* newIndexTerm ) {
        return _findReplacement<indri::lang::IndexTerm>( _terms, newIndexTerm );
      }

    };
  }
}

#endif // INDRI_DAGCOPIER_HPP

