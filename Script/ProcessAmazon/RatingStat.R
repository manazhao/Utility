# get the rating statistics

rating.file <- "/home/qzhao2/irkmwdex4-nfs/AmazonParsed/AmazonToyGame_parsed_rate.csv"
# read the csv file of which each line is read as,
# as <user_id>,<item_id>,<rating>,<date>,<# of userful vote>,<total # of vote>
ratings <- read.csv(file=rating.file,header=F)
# get the number of ratings for each item
item.nr.tab <- table(ratings[,2])
# build the histogram on the number of ratings
nr.cnt.tab <- table(item.nr.tab)
# get the accumulated percentage
accu.percent.tab <- nr.cnt.tab
for(i in 2:length(nr.cnt.tab)){
  accu.percent.tab[i] <- accu.percent.tab[i-1] + accu.percent.tab[i]
}
accu.percent.tab <- accu.percent.tab / sum(nr.cnt.tab)
