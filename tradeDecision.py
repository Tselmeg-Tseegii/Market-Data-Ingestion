import sys
import math
from river import compose, tree, stats, metrics
from collections import deque

COMBINED_CANDLE_LENGTH = 5

class Candle:
    def __init__(self, open_=0.0, high_=0.0, low_=0.0, close_=0.0, timestamp_=0):
        self.open_ = open_
        self.high_ = high_
        self.low_ = low_
        self.close_ = close_
        self.timestamp_ = timestamp_

    def fromLine(self, inputLine: str):
        inputLine = inputLine.strip("() \n")
        tokens = [t.strip() for t in inputLine.split(',')]
    
        self.open_ = float(tokens[0])
        self.high_ = float(tokens[1])
        self.low_ = float(tokens[2])
        self.close_ = float(tokens[3])
        self.timestamp_ = int(tokens[4])
        return self
        
    
    def isNull(self):
        return self.timestamp_ == 0

def addCandle(a: Candle, b: Candle) -> Candle:
    newCandle = Candle()
    newCandle.timestamp_ = b.timestamp_
    newCandle.high_ = max(a.high_, b.high_)
    newCandle.low_ = min(a.low_, b.low_)
    newCandle.open_ = a.open_
    newCandle.close_ = b.close_
    return newCandle

def createFeature(currCandle: Candle, preCandles: deque):
    features = {}
    #current candle features
    currBodyPercentChange = (currCandle.close_ - currCandle.open_) / currCandle.open_

    currUpperWiskPercentChange = (currCandle.high_ - max(currCandle.open_, currCandle.close_)) / currCandle.open_

    currLowerWiskPercentChange = -(currCandle.low_ - min(currCandle.open_, currCandle.close_)) / currCandle.open_

    features["currBodyPercentChange"] = currBodyPercentChange
    features["currUpperWiskPercentChange"] = currUpperWiskPercentChange
    features["currLowerWiskPercentChange"] = currLowerWiskPercentChange

    #the previous candle
    preCandle = preCandles[-1]
    features["preBodyPercentChange"] = (preCandle.close_ - preCandle.open_) / preCandle.open_
    #the gap between the previous high and the current candle's opening
    features["preHighPercentGap"] = (currCandle.open_ - preCandle.high_) / preCandle.high_

    # volatility
    currCandleRange = currCandle.high_ - currCandle.low_

    preVolatilityArr = [(candle.high_ - candle.low_) for candle in preCandles]
    preVolatility = sum(preVolatilityArr) / len(preVolatilityArr)

    currVolatilityRatio = currCandleRange / preVolatility
    features["currVolatilityRatio"] = currVolatilityRatio

    #the mean
    preCloseArr = [candle.close_ for candle in preCandles]
    meanClosePrice = sum(preCloseArr) / len(preCloseArr)
    features["diffMeanClosePrice"] = (currCandle.close_ - meanClosePrice) / meanClosePrice

    #the time of day
    minutes = (currCandle.timestamp_ // 60)
    features["5minuteStamp"] = minutes % ((24 * 60) // COMBINED_CANDLE_LENGTH)

    return features


model = compose.Pipeline(
    tree.HoeffdingTreeClassifier(
        grace_period = 50,       
        delta = 0.01  
    )
)

preCandles = deque(maxlen = 10)
preFeature = None

print("starting model")

while True:

    i = 0
    latestCombinedCandle = Candle()
    while i < COMBINED_CANDLE_LENGTH:
        line = sys.stdin.readline()
        if not line: 
            break 
        currCandle = Candle().fromLine(line)
        
        if latestCombinedCandle.isNull():
            latestCombinedCandle = currCandle
        else:
            latestCombinedCandle = addCandle(latestCombinedCandle, currCandle)
        i += 1
    
    if i != COMBINED_CANDLE_LENGTH:
        print("Python exiting due to not enough candles to combine")
        break

    if len(preCandles) != 10:
        preCandles.append(latestCombinedCandle)
        continue

    currFeature = createFeature(latestCombinedCandle, preCandles)

    if preFeature is not None:
        currRawBody = latestCombinedCandle.close_ - latestCombinedCandle.open_

        preVolatilityArr = [(candle.high_ - candle.low_) for candle in preCandles]
        preVolatility = sum(preVolatilityArr) / len(preVolatilityArr)

        noiseThreshold = preVolatility * 0.5 

        slippageCost = preVolatility * 0.3

        label = "NEUTRAL"
        
        if currRawBody > (noiseThreshold + slippageCost):
            label = "LONG"
        elif currRawBody < -(noiseThreshold + slippageCost):
            label = "SHORT"
            
        model.learn_one(preFeature, label)
    
    prediction = model.predict_one(currFeature)
    probs = model.predict_proba_one(currFeature)
    confidence = probs.get(prediction, 0.0)

    print(f"Time: {latestCombinedCandle.timestamp_} | Pred: {prediction} | Conf: {confidence:.2f}")

    preCandles.append(latestCombinedCandle)
    preFeature = currFeature