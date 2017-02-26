An outline of the collections in MongoDB and their metadata information:

existing databases

dailyData
|
`-- stocks:  indexes { SecID }
|
`-- futures: indexes { SecID }

binData
|
`-- stocks:  indexes { SecID, Date }
|
`-- futures: indexes { SecID, Date }
