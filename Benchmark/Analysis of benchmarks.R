setwd("/Users/tselmeg_otgonbayar/programming/ProjectDataIngestion/Benchmark")

newOBUpdateEvent <- scan("newImplem/orderBookUpdateFromEvent.txt")
newOBUpdateLoop <- scan("newImplem/orderBookLiveUpdateLoop.txt")
newOBWebRead <- scan("newImplem/orderBookWebSocketRead.txt")

newTVUpdateEvent <- scan("newImplem/tradeVolumeUpdateFromEvent.txt")
newTVUpdateLoop <- scan("newImplem/tradeVolumeUpdateLoop.txt")
newTVWebRead <- scan("newImplem/tradeVolumeWebSocketRead.txt")
newTVLifeTime <- scan("newImplem/tradeVolumeLifeTime.txt")

newNewTVLifeTime <- scan("newImplem/tradeVolumeLifeTimeParseInRead.txt")
newNewTVLifeTimeTwo <- scan("newImplem/tradeVolumeLifeTimeParseInReadTwo.txt")
newNewTVLifeTimeThree <- scan("newImplem/tradeVolumeLifeTimeParseInReadThree.txt")
newNewTVLifeTimeFour <- scan("newImplem/tradeVolumeLifeTimeParseInReadFour.txt")
newNewTVLifeTimeFive <- scan("newImplem/tradeVolumeLifeTimeParseInReadFive.txt")

oldOBUpdateEvent <- scan("oldImplem/orderBookUpdateFromEvent.txt")
oldOBUpdateLoop <- scan("oldImplem/orderBookUpdateLoop.txt")
oldOBWebRead <- scan("oldImplem/orderBookWebSocketRead.txt")

oldTVUpdateEvent <- scan("oldImplem/tradeVolumeUpdateFromEvent.txt")
oldTVUpdateLoop <- scan("oldImplem/tradeVolumeUpdateLoop.txt")
oldTVWebRead <- scan("oldImplem/tradeVolumeWebSocketRead.txt")
oldTVLifeTime <- scan("oldImplem/tradeVolumeLifeTime.txt")

TVQueueLengthWithoutConsume <- scan("newImplem/tradeVolumeQueueLengthWithoutConsume.txt")
TVQueueLengthWithConsume <- scan("newImplem/tradeVolumeQueueLengthWithConsume.txt")

newTVMap <- scan("newImplem/tradeVolumeLifeTimeMap.txt")

TVUpdaterWaitWithoutConsume <- scan("newImplem/tradeVolumeUpdaterWaitForWithoutConsume.txt")
TVUpdaterWaitWithConsume <- scan("newImplem/tradeVolumeUpdaterWaitForWithConsume.txt")

TVAbsTimeOfUpdate <- scan("newImplem/tradeVolumeUpdateFromEventAbsTime.txt")
TVAbsTimeOfUpdateTwo <- scan("newImplem/tradeVolumeUpdateFromEventAbsTimeTwo.txt")

rawTVConnection <- scan("rawWebSocketInputTradeVolume.txt")

par(mfrow = c(2, 2))

compare <- function(old, new) {
  old <- old[old <= 40000]
  new <- new[new <= 40000]
  
  cat("OLD\n")
  cat("Mean:  ", mean(old), "\n")
  cat("Median:", median(old), "\n")
  cat("SD:    ", sd(old), "\n")
  
  cat("NEW\n")
  cat("Mean:  ", mean(new), "\n")
  cat("Median:", median(new), "\n")
  cat("SD:    ", sd(new), "\n")
  
  maxOld = max(old)
  maxNew = max(new)
  maxAll = max(maxOld, maxNew)
  
  hist(old, breaks = 1000, xlim = c(0, 40000))
  plot(old, type = "p")
  hist(new, breaks = 1000, xlim = c(0, 40000))
  plot(new, type = "p")
}

compare(oldOBUpdateEvent, newOBUpdateEvent)
compare(oldOBWebRead, newOBWebRead)
compare(oldOBUpdateLoop, newOBUpdateLoop)

compare(oldTVUpdateEvent, newTVUpdateEvent)
compare(oldTVWebRead, newTVWebRead)
compare(oldTVUpdateLoop, newTVUpdateLoop)

compare(oldTVLifeTime, newTVLifeTime)
compare(oldTVLifeTime, newNewTVLifeTimeFour)

compare(newNewTVLifeTime, newNewTVLifeTimeTwo)
compare(rawTVConnection, newNewTVLifeTimeFive)

compare(TVQueueLengthWithoutConsume, TVQueueLengthWithConsume)

compare(TVUpdaterWaitWithoutConsume, TVUpdaterWaitWithConsume)

print(table(TVQueueLengthWithConsume))
print(table(TVQueueLengthWithoutConsume))

plot(TVAbsTimeOfUpdate)

x <- 1:length(TVAbsTimeOfUpdate)

lm(TVAbsTimeOfUpdate ~ x)

plot(TVAbsTimeOfUpdateTwo)

x2 <- 1:length(TVAbsTimeOfUpdateTwo)

fit <- lm(TVAbsTimeOfUpdateTwo ~ x2)

par(mfrow = c(1, 1))

abline(fit, col = "red")


timeBetween <- diff(TVAbsTimeOfUpdateTwo)

compare(newNewTVLifeTimeFour, timeBetween)

x3 <- 1:length(timeBetween)

fit <- lm(timeBetween ~ x3)

plot(timeBetween)
abline(fit, col = "red")
