// dependencies
var THREE = require('three');
var _ = require('lodash');
var async = require('async');
var AWS = require('aws-sdk');
var MongoClient = require('mongodb').MongoClient;

var s3 = new AWS.S3();

var mongoCollection = process.env.MONGO_COLLECTION;

exports.handler = function(event, context, callback) {
	var srcBucket = event.Records[0].s3.bucket.name;
	// Object key may have spaces or unicode non-ASCII characters.
	var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
	var keyNoFolder = srcKey.split('/')[1]
	var objectName = keyNoFolder.slice(0, keyNoFolder.lastIndexOf('.'))
	var mongoUser = process.env.MONGO_USER;
	var mongoPw = process.env.MONGO_PASSWORD;
	var mongoRawConnstring = process.env.MONGO_CONNSTRING;
	var mongoConnstring = "mongodb://" + mongoUser + ":" + mongoPw + "@" + mongoRawConnstring + "&authSource=admin";

	async.waterfall([
		function download(next) {
			// Download the image from S3 into a buffer.
			console.log("in download")
			s3.getObject({
					Bucket: srcBucket,
					Key: srcKey,
					ResponseContentType: 'application/json'
			},
			function(err, res) {
				if (err) next(err);
				else {
					connectAndUpdateDb(objectName, 202, 'Model element data received, beginning tessellation', mongoConnstring, {"next": next, "arguments": [null,res]})
					//next(null, res)
				}
			});
		},
		function getTagset(response, next) {
			console.log("in tagset")
			var parsedData = JSON.parse(response.Body.toString())
			s3.getObjectTagging({
					Bucket: srcBucket,
					Key: srcKey
			},
			function(err, tagResp) {
				if (err) next(err);
				else {
					connectAndUpdateDb(objectName, 202, 'Model metadata received', mongoConnstring, {"next": next, "arguments": [null,parsedData,tagResp.TagSet]})
					//next(null, parsedData, tagResp.TagSet)
				}
			});
		},
		function tesselateAndUpload(data, tagset, next) {
			console.log("in tesselate")
			if (!('payload' in data && 'planarElements' in data.payload)) {
				next("invalid json file")
			}
			for (var i=0; i < data.payload.planarElements.length; i++) {
				data.payload.planarElements[i].faceVertices = [];
				var _vertices = [];
				for (var v=0; v < data.payload.planarElements[i].vertices.length; v++) {
					_vertices.push(new THREE.Vector3(...data.payload.planarElements[i].vertices[v]));
				}
				//add index 0 back to path to ensure closed loop.
				if (_vertices.length > 0) {
					var _closedLoop = _vertices[0].x === _vertices[_vertices.length-1].x && _vertices[0].y === _vertices[_vertices.length-1].y && _vertices[0].z === _vertices[_vertices.length-1].z;
					if (!_closedLoop) _vertices.push(new THREE.Vector3(...data.payload.planarElements[i].vertices[0]))
				}
				var shellShape = new THREE.Shape(_vertices);
				var shellGeo = new THREE.ShapeGeometry(shellShape)
				var shellFaces = shellGeo.faces 
				if (_.isEmpty(shellFaces)) {
					//the element is a vertical wall, shape geometry cannot tesselate
					shellFaces = rotationTesselation(_vertices)
					if (_.isEmpty(shellFaces)) {
						console.log("WARNING: No faces could be generated for the element at index: " + i)
						console.log(_vertices)
					}
				}
				data.payload.planarElements[i].faceVertices = _.map(shellFaces, function(f) {return [f.a,f.b,f.c]});
			}
			var tagMap = _.reduce(tagset, function(r,o) {
				r[o.Key] = o.Value
				return r
			},{});
			var dstBucket = tagMap.destination;
			delete tagMap.destination;
			if ("destination" in data.modelInformation) delete data.modelInformation.destination
			var tags = _.join(_.map(tagMap,function(v,k) {return k+"="+v}),'&')
			var putParams = {
				Bucket: dstBucket,
				Key: srcKey,
				Body: JSON.stringify(data),
				ContentType: 'application/json'
			}
			if (tags !== "") putParams.Tagging = tags;
			s3.putObject(putParams, next);
		}], 
		function(err) {
			//notes: need to set dist vars with tag info for destination bucket/key
			if (err) {
				console.error(
					'Unable to tesselate ' + srcBucket + '/' + srcKey + ' due to an error: ' + err
				);
				connectAndUpdateDb(objectName, 500, 'Unable to tesselate and upload model due to an error: ' + err, mongoConnstring, {})
			} 
			else {
				console.log(
					'Successfully tesselated ' + srcBucket + '/' + srcKey
				);
				connectAndUpdateDb(objectName, 200, 'Tessellation complete, model has been successfully uploaded', mongoConnstring, {})
			}
			// callback(null, "message");
		}
	);
};

function connectAndUpdateDb(name, newStatus, newLog, mongoConnstring_, nextContext) {
	MongoClient.connect(mongoConnstring_, function(err, db) {
		console.log("connected to mongodb");
		var col = db.collection(mongoCollection);
		col.findOneAndUpdate({name: name},
		{'$set': {'status': newStatus}, '$addToSet': {'log' : newLog}},
		function(err, res) {
			if (err) {
				if ("next" in nextContext) nextContext.next(err)
			}
			db.close()
			if ("next" in nextContext) {
				nextContext.next(...nextContext.arguments)
			}
		})
	})
}

function rotationTesselation(_vertices) {
	var _faces = []
	if (_vertices.length >= 3) {
		for (var v=0; v < _vertices.length-1; v++) {
			var ref = _vertices[v]
			for (var i=0; i < _vertices.length-1; i++) {
				if (v != i) {
					vector = new THREE.Vector3(_vertices[i].x - ref.x, _vertices[i].y - ref.y, _vertices[i].z - ref.z);
					if (vector.z === 0) break;
				}
			}
			if (vector.z === 0) break;
		}
		if (vector.z === 0) {
			for (var i=0; i < _vertices.length; i++) {
				_vertices[i].applyAxisAngle(vector.normalize(), Math.PI/2)
			}
			var rotatedShape = new THREE.Shape(_vertices);
			var rotatedGeo = new THREE.ShapeGeometry(rotatedShape)
			_faces = rotatedGeo.faces
		}
	}
	return _faces
}