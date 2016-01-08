// 
// Warning: This file is automatically generated
// 

#ifndef INDRI_WALKER_HPP
#define INDRI_WALKER_HPP

namespace indri { 
  namespace lang { 
    class Walker { 
    public:
      virtual ~Walker();
      virtual void defaultBefore( class Node* n );
      virtual void defaultAfter( class Node* n );

      virtual void before( class IndexTerm* n );
      virtual void after( class IndexTerm* n );
      virtual void before( class RawScorerNode* n );
      virtual void after( class RawScorerNode* n );
      virtual void before( class TermFrequencyScorerNode* n );
      virtual void after( class TermFrequencyScorerNode* n );
      virtual void before( class CombineNode* n );
      virtual void after( class CombineNode* n );
      virtual void before( class WeightNode* n );
      virtual void after( class WeightNode* n );
      virtual void before( class ContextCounterNode* n );
      virtual void after( class ContextCounterNode* n );
      virtual void before( class ContextSimpleCounterNode* n );
      virtual void after( class ContextSimpleCounterNode* n );
      virtual void before( class ScoreAccumulatorNode* n );
      virtual void after( class ScoreAccumulatorNode* n );
      virtual void before( class ContextInclusionNode* n );
      virtual void after( class ContextInclusionNode* n );
   };
 }
}

#endif // INDRI_WALKER_HPP

