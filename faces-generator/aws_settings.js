var mongoUser = "ArupLAMongo"
var mongoPw = "m2nuvwNp56HRfNk5"

module.exports = {
  mongoUser: mongoUser,
  mongoPw: mongoPw,
  mongoConnstring: "mongodb://" + mongoUser + ":" + mongoPw + "@cluster0-shard-00-00-7hl8b.mongodb.net:27017,cluster0-shard-00-01-7hl8b.mongodb.net:27017,cluster0-shard-00-02-7hl8b.mongodb.net:27017/rubisandbox?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin"
}

//if testing locally use below within GenerateFaces.js
// var awsSettings = require('./aws_settings')
