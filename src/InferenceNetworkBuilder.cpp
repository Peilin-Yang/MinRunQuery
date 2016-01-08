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
// InferenceNetworkBuilder
//
// 8 March 2004 -- tds
//

#include "indri/InferenceNetworkBuilder.hpp"
#include "indri/ContextSimpleCountAccumulator.hpp"
#include "indri/ScoredExtentAccumulator.hpp"
#include "indri/WeightedAndNode.hpp"
#include "indri/NullScorerNode.hpp"
#include "indri/WeightedAndNode.hpp"
#include "indri/NullListNode.hpp"
#include "indri/TermScoreFunctionFactory.hpp"
#include "indri/TermFrequencyBeliefNode.hpp"

#include <cmath>
#include <stdexcept>

indri::query::TermScoreFunction* indri::infnet::InferenceNetworkBuilder::_buildTermScoreFunction( const std::string& smoothing, double occurrences, double contextSize, int documentOccurrences, int documentCount ) const {
  double collectionFrequency;
  return indri::query::TermScoreFunctionFactory::get( smoothing, occurrences, contextSize, documentOccurrences, documentCount );
}

indri::infnet::InferenceNetworkBuilder::InferenceNetworkBuilder( indri::collection::Repository& repository, indri::lang::ListCache& cache, int resultsRequested, int maxWildcardTerms ) :
  _repository(repository),
  _cache(cache),
  _network( new indri::infnet::InferenceNetwork( repository ) ),
  _resultsRequested( resultsRequested ),
  _maxWildcardTerms( maxWildcardTerms )
{
}

indri::infnet::InferenceNetworkBuilder::~InferenceNetworkBuilder() {
  delete _network;
}

indri::infnet::InferenceNetwork* indri::infnet::InferenceNetworkBuilder::getNetwork() {
  return _network;
}

void indri::infnet::InferenceNetworkBuilder::defaultAfter( indri::lang::Node* node ) {
  LEMUR_THROW( LEMUR_INTERNAL_ERROR, "InferenceNetworkBuilder found a node type that it didn't know what to do with" );
}

static bool inferencenetworkbuilder_is_stopword( indri::infnet::ListIteratorNode* node ) {
  indri::infnet::NullListNode* nullNode = dynamic_cast<indri::infnet::NullListNode*>(node);
  return nullNode && nullNode->isStopword();
}

static bool inferencenetworkbuilder_contains_stopwords( const std::vector<indri::infnet::ListIteratorNode*>& nodes ) {
  // scan for stopwords
  int stopwords = 0;

  for( size_t i=0; i<nodes.size(); i++ ) {
    if( inferencenetworkbuilder_is_stopword(nodes[i]) )
      stopwords++;
  }

  return stopwords != 0;
}

static int inferencenetworkbuilder_find_stopwords_left( const std::vector<indri::infnet::ListIteratorNode*>& nodes ) {
  size_t begin;

  for( begin=0; begin<nodes.size(); begin++ ) {
    if( inferencenetworkbuilder_is_stopword(nodes[begin]) )
      continue;

    break;
  }

  return int(begin);
}

static int inferencenetworkbuilder_find_stopwords_right( const std::vector<indri::infnet::ListIteratorNode*>& nodes ) {
  int end;

  if( nodes.size() == 0 )
    return 0;

  for( end=(int)nodes.size()-1; end>=0; end-- ) {
    if( inferencenetworkbuilder_is_stopword(nodes[end]) )
      continue;

    break;
  }

  return end+1;
}

static void inferencenetworkbuilder_find_stopwords_stretch( const std::vector<indri::infnet::ListIteratorNode*>& nodes, int& begin, int& end ) {
  for( begin=0; begin<int(nodes.size()); begin++ ) {
    if( inferencenetworkbuilder_is_stopword(nodes[begin]) )
      break;
  }

  for( end=begin+1; end<int(nodes.size()); end++ ) {
    if( !inferencenetworkbuilder_is_stopword(nodes[end]) )
      break;
  }
}

void indri::infnet::InferenceNetworkBuilder::after( indri::lang::ContextSimpleCounterNode* contextSimpleCounterNode ) {
  if( _nodeMap.find( contextSimpleCounterNode ) == _nodeMap.end() ) {
    ContextSimpleCountAccumulator* contextCount = 0;

    contextCount = new ContextSimpleCountAccumulator( contextSimpleCounterNode->nodeName(),
                                                      contextSimpleCounterNode->terms(),
                                                      contextSimpleCounterNode->field(),
                                                      contextSimpleCounterNode->context() );

    _network->addEvaluatorNode( contextCount );
    _nodeMap[ contextSimpleCounterNode ] = contextCount;
  }
}

void indri::infnet::InferenceNetworkBuilder::after( indri::lang::ScoreAccumulatorNode* scoreAccumulatorNode ) {
  if( _nodeMap.find( scoreAccumulatorNode ) == _nodeMap.end() ) {
    indri::lang::Node* c = scoreAccumulatorNode->getChild();
    BeliefNode* child = dynamic_cast<BeliefNode*>(_nodeMap[c]);
    ScoredExtentAccumulator* accumulator = new ScoredExtentAccumulator( scoreAccumulatorNode->nodeName(), child, _resultsRequested );

    _network->addEvaluatorNode( accumulator );
    _network->addComplexEvaluatorNode( accumulator );
    _nodeMap[ scoreAccumulatorNode ] = accumulator;
  }
}


void indri::infnet::InferenceNetworkBuilder::after( indri::lang::TermFrequencyScorerNode* termScorerNode ) {
  if( _nodeMap.find( termScorerNode ) == _nodeMap.end() ) {
    indri::infnet::BeliefNode* belief = 0;
    indri::query::TermScoreFunction* function = 0;

    function = _buildTermScoreFunction( termScorerNode->getSmoothing(),
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

    _network->addScoreFunction( function );
    _network->addBeliefNode( belief );
    _nodeMap[termScorerNode] = belief;
  }
}

void indri::infnet::InferenceNetworkBuilder::after( indri::lang::RawScorerNode* rawScorerNode ) {
  if( _nodeMap.find( rawScorerNode ) == _nodeMap.end() ) {
    BeliefNode* belief;
    InferenceNetworkNode* untypedRawExtentNode = _nodeMap[rawScorerNode->getRawExtent()];
    InferenceNetworkNode* untypedContextNode = _nodeMap[rawScorerNode->getContext()];
    ListIteratorNode* iterator = dynamic_cast<ListIteratorNode*>(untypedRawExtentNode);

    indri::query::TermScoreFunction* function = 0;

    function = _buildTermScoreFunction( rawScorerNode->getSmoothing(),
                                        rawScorerNode->getOccurrences(),
                                        rawScorerNode->getContextSize(),
                                        rawScorerNode->getDocumentOccurrences(),
                                        rawScorerNode->getDocumentCount() );
    
    if( rawScorerNode->getOccurrences() > 0 && iterator != 0 ) {
      ListIteratorNode* rawIterator = 0;
      ListIteratorNode* context = dynamic_cast<ListIteratorNode*>(untypedContextNode);

      if( context ) {
        rawIterator = iterator;
        //iterator = new ExtentInsideNode( "", rawIterator, context );
        _network->addListNode( iterator );
      }
      
      // this is here to turn max-score off for this term
      // only frequency lists are "max-scored"
      double maximumScore = INDRI_HUGE_SCORE;
      double maximumBackgroundScore = INDRI_HUGE_SCORE;
    } else {
      belief = new NullScorerNode( rawScorerNode->nodeName(), *function );
    }

    _network->addScoreFunction( function );
    _network->addBeliefNode( belief );
    _nodeMap[rawScorerNode] = belief;
  }
}

void indri::infnet::InferenceNetworkBuilder::after( indri::lang::WeightNode* weightNode ) {
  if( _nodeMap.find( weightNode ) == _nodeMap.end() ) {
    const std::vector< std::pair<double, indri::lang::ScoredExtentNode*> >& children = weightNode->getChildren();
    WeightedAndNode* wandNode = new WeightedAndNode( weightNode->nodeName() );
    // normalize over absolute values
    double totalWeights = 0;
    for( size_t i=0; i<children.size(); i++ ) {
      totalWeights += fabs(children[i].first);
    }

    for( size_t i=0; i<children.size(); i++ ) {
      wandNode->addChild( children[i].first / totalWeights,
                          dynamic_cast<BeliefNode*>( _nodeMap[children[i].second] ) );
    }

    wandNode->doneAddingChildren();

    _network->addBeliefNode( wandNode );
    _nodeMap[weightNode] = wandNode;
  }
}

void indri::infnet::InferenceNetworkBuilder::after( indri::lang::CombineNode* combineNode ) {
  if( _nodeMap.find( combineNode ) == _nodeMap.end() ) {
    const std::vector<indri::lang::ScoredExtentNode*>& children = combineNode->getChildren();
    double weight = 1. / double(children.size());

    std::vector<BeliefNode*> translation = _translate<BeliefNode,indri::lang::ScoredExtentNode>( children );
    WeightedAndNode* wandNode = new WeightedAndNode( combineNode->nodeName() );

    for( size_t i=0; i<children.size(); i++ ) {
      wandNode->addChild( weight, translation[i] );
    }

    wandNode->doneAddingChildren();

    _network->addBeliefNode( wandNode );
    _nodeMap[combineNode] = wandNode;
  }
}
