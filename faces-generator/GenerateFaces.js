// dependencies
var THREE = require('three');
var _ = require('lodash');
var async = require('async');
var AWS = require('aws-sdk');

var s3 = new AWS.S3();

exports.handler = function(event, context, callback) {
    var srcBucket = event.Records[0].s3.bucket.name;
    // Object key may have spaces or unicode non-ASCII characters.
    var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));  
    async.waterfall([
        function download(next) {
            // Download the image from S3 into a buffer.
            s3.getObject({
                    Bucket: srcBucket,
                    Key: srcKey,
                    ResponseContentType: 'application/json'
                },
                next);
            },
        function getTagset(response, next) {
        	var parsedData = JSON.parse(response.Body.toString())
            s3.getObjectTagging({
                    Bucket: srcBucket,
                    Key: srcKey
                },
                function(err, tagResp) {
                	if (err) next(err);
                	else {
                		next(null, parsedData, tagResp.TagSet)
                	}
                });
        	},

        function tesselateAndUpload(data, tagset, next) {
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
       	    	data.payload.planarElements[i].faceVertices = _.map(shellGeo.faces, function(f) {return [f.a,f.b,f.c]});
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

        }], function(err) {
       		//notes: need to set dist vars with tag info for destination bucket/key
            if (err) {
                console.error(
                    'Unable to tesselate ' + srcBucket + '/' + srcKey + ' due to an error: ' + err
                );
            } else {
                console.log(
                    'Successfully tesselated ' + srcBucket + '/' + srcKey
                );
            }

            callback(null, "message");
       	}
    );    
};