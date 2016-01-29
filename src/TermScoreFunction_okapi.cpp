#include "indri/TermScoreFunction.hpp"
#include <cmath>

// Okapi

void indri::query::TermScoreFunction::_preCompute() {
  _modelParas["_idfTimesK1PlusOne"] = _modelParas["idf"] * ( _modelParas["k1"] + 1 );
  _modelParas["_k1TimesOneMinusB"] = _modelParas["k1"] * (1 - _modelParas["b"]);
  _modelParas["_bOverAvgDocLength"] = _modelParas["b"] / _modelParas["avdl"];
  _modelParas["_k1TimesBOverAvgDocLength"] = _modelParas["k1"] * _modelParas["_bOverAvgDocLength"];
  _modelParas["_termWeightTimesidfTimesK1PlusOne"] = _modelParas["_idfTimesK1PlusOne"];
}

indri::query::TermScoreFunction::TermScoreFunction( double collectionOccurence, double collectionSize, double documentOccurrences, 
    double documentCount, std::map<std::string, double>& paras ) {
  _collectionOccurence = collectionOccurence;
  _collectionSize = collectionSize;
  _documentOccurrences = documentOccurrences;
  _documentCount = documentCount;
  _modelParas = paras;
  _modelParas["idf"] = log( ( documentCount - documentOccurrences + 0.5 ) / ( documentOccurrences + 0.5 ) );
  _modelParas["avdl"] = collectionSize / double(documentCount);
  _preCompute();
}


double indri::query::TermScoreFunction::scoreOccurrence( double occurrences, int documentLength ) {
  double termWeight = (_modelParas["k3"] + 1) * qtf / (_modelParas["k3"] + qtf);
  double numerator = _modelParas["_termWeightTimesidfTimesK1PlusOne"] * occurrences * termWeight;
  double denominator = occurrences + _modelParas["_k1TimesOneMinusB"] + _modelParas["_k1TimesBOverAvgDocLength"] * documentLength;
  return numerator / denominator; 
}
