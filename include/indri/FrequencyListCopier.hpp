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
// FrequencyListCopier
//
// 24 August 2004 -- tds
//
// Finds IndexTerm nodes that only need to return frequency information,
// not positions, and inserts IndexFrequencyTerms instead.
//

#ifndef INDRI_FREQUENCYLISTCOPIER_HPP
#define INDRI_FREQUENCYLISTCOPIER_HPP

#include "ListCache.hpp"
namespace indri
{
  namespace lang
  {
    
    class FrequencyListCopier : public indri::lang::Copier {
    private:
      std::vector<indri::lang::Node*> _nodes;
      std::stack<indri::lang::Node*> _disqualifiers;
      indri::lang::IndexTerm* _lastTerm;
      bool _disqualifiedTree;

      ListCache* _listCache;

    public:
      FrequencyListCopier( ListCache* listCache ) : _listCache(listCache), _lastTerm(0), _disqualifiedTree(false) {}

      indri::lang::Node* defaultAfter( indri::lang::Node* oldNode, indri::lang::Node* newNode ) {
        if( _disqualifiers.size() && oldNode == _disqualifiers.top() )
          _disqualifiers.pop();
    
        _nodes.push_back( newNode );
        return newNode;
      }

      ~FrequencyListCopier() {
        indri::utility::delete_vector_contents<indri::lang::Node*>( _nodes );
      }

      void before( indri::lang::ContextCounterNode* context ) {
        if( context->getContext() != NULL ) {
          _disqualifiedTree = true;
        }
      }

      indri::lang::Node* after( indri::lang::IndexTerm* oldNode, indri::lang::IndexTerm* newNode ) {
        _lastTerm = newNode;
        return defaultAfter( oldNode, newNode );
      }

      void before( indri::lang::RawScorerNode* oldNode, indri::lang::RawScorerNode* newNode ) {
        _lastTerm = 0;
        _disqualifiedTree = false;
      }

      indri::lang::Node* after( indri::lang::RawScorerNode* oldNode, indri::lang::RawScorerNode* newNode ) {
        indri::lang::Node* result = 0;

        if( _lastTerm && !_disqualifiers.size() && !_disqualifiedTree && oldNode->getContext() == NULL ) {
          indri::lang::TermFrequencyScorerNode* scorerNode;
          // there's a term to score, and nothing to disqualify us from doing frequency scoring
          scorerNode = new indri::lang::TermFrequencyScorerNode( _lastTerm->getText(),
                                                                 _lastTerm->getStemmed() );

          scorerNode->setNodeName( oldNode->nodeName() );
          scorerNode->setSmoothing( oldNode->getSmoothing() );
          scorerNode->setStatistics( oldNode->getOccurrences(), oldNode->getContextSize(), oldNode->getDocumentOccurrences(), oldNode->getDocumentCount() );

          delete newNode;
          result = defaultAfter( oldNode, scorerNode );
        } else if( !_disqualifiers.size() ) {
          ListCache::CachedList* list = 0; 

          if( _listCache )
            list = _listCache->find( newNode->getRawExtent(), newNode->getContext() );
      
          if( list ) {
            
          } else {
            result = defaultAfter( oldNode, newNode );
          }
        } else {
          result = defaultAfter( oldNode, newNode );
        }

        _disqualifiedTree = false;
        return result; 
      }
    };
  }
}

#endif // INDRI_FREQUENCYLISTCOPIER_HPP

