setwd("/Users/tselmeg_otgonbayar/programming/ProjectDataIngestion/Benchmark")

newOBUpdateEvent <- scan("newImplem/orderBookUpdateFromEvent.txt")
newOBUpdateLoop <- scan("newImplem/orderBookLiveUpdateLoop.txt")
newOBWebRead <- scan("newImplem/orderBookWebSocketRead.txt")

newTVUpdateEvent <- scan("newImplem/tradeVolumeUpdateFromEvent.txt")
newTVUpdateLoop <- scan("newImplem/tradeVolumeUpdateLoop.txt")
newTVWebRead <- scan("newImplem/tradeVolumeWebSocketRead.txt")
newTVLifeTime <- scan("newImplem/tradeVolumeLifeTime.txt")

oldOBUpdateEvent <- scan("oldImplem/orderBookUpdateFromEvent.txt")
oldOBUpdateLoop <- scan("oldImplem/orderBookUpdateLoop.txt")
oldOBWebRead <- scan("oldImplem/orderBookWebSocketRead.txt")

oldTVUpdateEvent <- scan("oldImplem/tradeVolumeUpdateFromEvent.txt")
oldTVUpdateLoop <- scan("oldImplem/tradeVolumeUpdateLoop.txt")
oldTVWebRead <- scan("oldImplem/tradeVolumeWebSocketRead.txt")
oldTVLifeTime <- scan("oldImplem/tradeVolumeLifeTime.txt")

par(mfrow = c(1, 2))

compare <- function(old, new) {
  old <- old[old <= 500000]
  new <- new[new <= 500000]
  
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
  
  hist(old, breaks = 1000, xlim = c(0, 500000))
  hist(new, breaks = 1000, xlim = c(0, 500000))
}

compare(oldOBUpdateEvent, newOBUpdateEvent)
compare(oldOBWebRead, newOBWebRead)
compare(oldOBUpdateLoop, newOBUpdateLoop)

compare(oldTVUpdateEvent, newTVUpdateEvent)
compare(oldTVWebRead, newTVWebRead)
compare(oldTVUpdateLoop, newTVUpdateLoop)

compare(oldTVLifeTime, newTVLifeTime)
