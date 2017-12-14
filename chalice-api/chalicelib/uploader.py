
from datetime import datetime
import json
import os
import boto3
from aws_settings import aws_resources
from botocore.client import Config
import pymongo

s3 = boto3.resource('s3')

client = pymongo.MongoClient(aws_resources['mongo_connstring'])
db = client.rubisandbox
collection = db.test

settings = {
	'site-bucket': aws_resources['site-bucket'],
	'data-folder': aws_resources['data-folder'],
	'staging-bucket': aws_resources['staging-bucket'],
}

source_bucket = s3.Bucket(settings['site-bucket'])
faces_staging_bucket = s3.Bucket(settings['staging-bucket'])

class Error(Exception):
  pass

class MissingModelInfoKeysError(Error):
  pass

class InvalidUnitsError(Error):
  pass

class MissingPayloadKeysError(Error):
  pass

class EmptyVerticesError(Error):
  pass

class EmptyMetadataError(Error):
  pass

class AlreadyInBucketError(Error):
  pass

def updateMongoStatus(name, new_status, new_log, new_date):
	db.test.find_one_and_update({'name': name}, {'$push': {'status': new_status, 'log' : new_log, 'date': new_date}}, upsert=True, return_document=pymongo.ReturnDocument.AFTER)

def timestampify(s):
	t = datetime.now().strftime('%y%m%d_%H%M%S')
	return s + "_" + t

def validateJson(postData):
    #postData defined in app.py
	try:
		nonLinearToggle = False
		#WrongNameError necessary to check that upload name matches name within JSON?
		if "modelInformation" not in postData or "payload" not in postData:
			raise KeyError
		if "units" not in postData["modelInformation"] or "name" not in postData["modelInformation"]:
			raise MissingModelInfoKeysError
		postData["modelInformation"]["name"] = timestampify(postData["modelInformation"]["name"])
		updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary structure within modelInformation", datetime.utcnow())
		valid_units = ["metric", "imperial"]
		if postData["modelInformation"]["units"] not in valid_units:
			raise InvalidUnitsError
		updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has valid units", datetime.utcnow())
		payload_elems = postData["payload"]
		planar_elements = []
		linear_elements = []
		for i in payload_elems:
			if "vertices" not in i.keys() or "metadata" not in i.keys():
				raise MissingPayloadKeysError
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary keys within payload", datetime.utcnow())
			if not i["vertices"]:
			  	raise EmptyVerticesError
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary vertices", datetime.utcnow())
			if not i["metadata"]:
			  	raise EmptyMetadataError
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary metadata", datetime.utcnow())
			if len(i["vertices"]) > 2:
				planar_elements.append(i)
			else:
				linear_elements.append(i)
		postData["payload"] = {
			"linearElements": linear_elements,
			"planarElements": planar_elements
		}
		for obj in source_bucket.objects.all():
			if (os.path.basename(obj.key)).lower() == (postData["modelInformation"]["name"] + ".json").lower():
				raise AlreadyInBucketError
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has a unique filename", datetime.utcnow())
		try:
			if len(planar_elements) > 0:
				nonLinearToggle = True
				postData["modelInformation"]["destination"] = settings['site-bucket']
				destination_bucket = faces_staging_bucket
			else:
				destination_bucket = source_bucket

			#process model info and create s3 tags for filtering purposes.
			tags = '&'.join([str(k)+"="+str(v) for k,v in postData["modelInformation"].iteritems()])
			if tags:
				destination_bucket.put_object(Key=settings["data-folder"] + "/" + postData["modelInformation"]["name"] + ".json", Body=json.dumps(postData), Tagging=tags)
			else:
				destination_bucket.put_object(Key=settings["data-folder"] + "/" + postData["modelInformation"]["name"] + ".json", Body=json.dumps(postData))
		except:
			return {"success": False}
		if nonLinearToggle:
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON is valid; waiting for tesselation", datetime.utcnow())
		else:
			updateMongoStatus(postData["modelInformation"]["name"], 200, "JSON upload is complete", datetime.utcnow())
			return {"success": True}
	except KeyError:
		return {"error": "'modelInformation' and 'payload' properties are required"}
	except MissingModelInfoKeysError:
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'modelInformation' must contain 'units' and 'name'", datetime.utcnow())
		return {"error": "'modelInformation' must contain 'units' and 'name'"}
	except InvalidUnitsError:
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: invalid units; 'units' values must be either 'imperial' or 'metric'", datetime.utcnow())
	  	return {"error": "'units' values must be either 'imperial' or 'metric'"}
	except MissingPayloadKeysError:
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'payload' must include 'vertices' and 'metadata' for each element", datetime.utcnow())
	  	return {"error": "'payload' must include 'vertices' and 'metadata' for each element"}
	except EmptyVerticesError:
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'vertices' data is empty", datetime.utcnow())
	  	return {"error": "'vertices' data is empty"}
	except EmptyMetadataError:
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'metadata' field is empty", datetime.utcnow())
	  	return {"error": "'metadata' field is empty"}
	except AlreadyInBucketError:
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: File with this name already exists in S3 bucket. Please rename file and try again.", datetime.utcnow())
		return {"error": "File with this name already exists in S3 bucket. Please rename file and try again." }
	except:
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: unknown error occurred", datetime.utcnow())
		return {"error": "unknown error occurred"}
