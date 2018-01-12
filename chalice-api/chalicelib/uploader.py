from datetime import datetime
import json
import os
import boto3
from aws_settings import aws_resources
from botocore.client import Config
import pymongo
import re

s3 = boto3.resource('s3')

settings = {
	'site-bucket': aws_resources['site-bucket'],
	'data-folder': aws_resources['data-folder'],
	'staging-bucket': aws_resources['staging-bucket']
}

source_bucket = s3.Bucket(settings['site-bucket'])
faces_staging_bucket = s3.Bucket(settings['staging-bucket'])

client = pymongo.MongoClient(aws_resources['mongo_connstring'])
db = client.rubisandbox
collection = db.test

class Error(Exception):
  pass

class MissingModelInfoKeysError(Error):
  pass

class TimestampError(Error):
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

# def timestampify(s):
# 	t = datetime.now().strftime('%y%m%d_%H%M%S')
# 	return s + "_" + t

def updateMongoStatus(name, new_status, new_log):
    db.test.find_one_and_update({'name': name}, {'$addToSet': {'status': new_status, 'log' : new_log}}, upsert=True, return_document=pymongo.ReturnDocument.AFTER)

def validateJson(postData):
    #postData defined in app.py
	# has_planar_elems = False
	try:
		if "modelInformation" not in postData or "payload" not in postData:
			raise KeyError
		if "units" not in postData["modelInformation"] or "name" not in postData["modelInformation"]:
			raise MissingModelInfoKeysError
		# postData["modelInformation"]["name"] = timestampify(postData["modelInformation"]["name"])
		print("JSON has necessary structure within modelInformation")
		updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary structure within modelInformation")
		print("filename: " + postData["modelInformation"]["name"])
		fileTimeStamp = postData["modelInformation"]["name"][-15:]
		print(fileTimeStamp)
		timeStampRegEx = re.compile(r'\D_\d\d\d\d\d\d_\d\d\d\d\d\d')
		mo = timeStampRegEx.match(fileTimeStamp)
		if mo == None or fileTimeStamp[1] != '_' or fileTimeStamp[8] != '_':
			raise TimestampError
		print("File has one timestamp appended to it")
		updateMongoStatus(postData["modelInformation"]["name"], 202, "File has one timestamp appended to it")
		valid_units = ["metric", "imperial"]
		if postData["modelInformation"]["units"] not in valid_units:
			raise InvalidUnitsError
		print("JSON has valid units")
		updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has valid units")
		payload_elems = postData["payload"]
		planar_elements = []
		linear_elements = []
		for i in payload_elems:
			if "vertices" not in i.keys() or "metadata" not in i.keys():
				raise MissingPayloadKeysError
			print("JSON has necessary keys within payload")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary keys within payload")
			if not i["vertices"]:
			  	raise EmptyVerticesError
			print("JSON has necessary vertices")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary vertices")
			if not i["metadata"]:
			  	raise EmptyMetadataError
			print("JSON has necessary metadata")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has necessary metadata")
			if len(i["vertices"]) > 2:
				# has_planar_elems = True
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
			print("JSON has a unique filename")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON has a unique filename")
		try:
			if len(planar_elements) > 0:
				postData["modelInformation"]["destination"] = settings['site-bucket']
				destination_bucket = faces_staging_bucket
				print("JSON is valid; waiting for tesselation")
				updateMongoStatus(postData["modelInformation"]["name"], 202, "JSON is valid; waiting for tesselation")
			else:
				destination_bucket = source_bucket
				print("Success, JSON upload is complete")
				updateMongoStatus(postData["modelInformation"]["name"], 200, "Success, JSON upload is complete")
			#process model info and create s3 tags for filtering purposes.
			copy_modelinfo_dict = eval(repr(postData["modelInformation"]))
			copy_modelinfo_dict.pop('untagged', None)
			tags = '&'.join([str(k)+"="+str(v) for k,v in copy_modelinfo_dict.iteritems()])
			print(tags)
			if tags:
				destination_bucket.put_object(Key=settings["data-folder"] + "/" + postData["modelInformation"]["name"] + ".json", Body=json.dumps(postData), Tagging=tags)
			else:
				destination_bucket.put_object(Key=settings["data-folder"] + "/" + postData["modelInformation"]["name"] + ".json", Body=json.dumps(postData))
		except:
			return {"success": False}
		return {"success": True}
	except KeyError:
		print("error--'modelInformation' and 'payload' properties are required")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'modelInformation' and 'payload' properties are required")
		return {"error": "'modelInformation' and 'payload' properties are required"}
	except MissingModelInfoKeysError:
		print("error--'modelInformation' must contain 'units' and 'name'")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'modelInformation' must contain 'units' and 'name'")
		return {"error": "'modelInformation' must contain 'units' and 'name'"}
	except TimestampError:
		print("error--timestamp is either missing or has been manually appended to file")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: timestamp is either missing or has been manually appended to file")
		return {"error": "timestamp is either missing or has been manually appended to file"}
	except InvalidUnitsError:
		print("error--'units' values must be either 'imperial' or 'metric'")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'units' values must be either 'imperial' or 'metric'")
	  	return {"error": "'units' values must be either 'imperial' or 'metric'"}
	except MissingPayloadKeysError:
		print("error--'payload' must include 'vertices' and 'metadata' for each element")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'payload' must include 'vertices' and 'metadata' for each element")
	  	return {"error": "'payload' must include 'vertices' and 'metadata' for each element"}
	except EmptyVerticesError:
		print("error--'vertices' data is empty")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'vertices' data is empty")
	  	return {"error": "'vertices' data is empty"}
	except EmptyMetadataError:
		print("error--'metadata' field is empty")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: 'metadata' field is empty")
	  	return {"error": "'metadata' field is empty"}
	except AlreadyInBucketError:
		print("error--File with this name already exists in S3 bucket. Please rename file and try again.")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: File with this name already exists in S3 bucket. Please rename file and try again.")
		return {"error": "File with this name already exists in S3 bucket. Please rename file and try again."}
	except:
		print("error--unknown error occurred")
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: unknown error occurred")
		return {"error": "unknown error occurred"}
