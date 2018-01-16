from datetime import datetime
import json
import os
import boto3
from botocore.client import Config
import pymongo
import re
from chalicelib.aws_settings import aws_resources

s3 = boto3.resource('s3')

settings = {
	'site-bucket': aws_resources['SITE_BUCKET'],
	'data-folder': aws_resources['DATA_FOLDER'],
	'staging-bucket': aws_resources['STAGING_BUCKET']
}

source_bucket = s3.Bucket(settings['site-bucket'])
faces_staging_bucket = s3.Bucket(settings['staging-bucket'])

client = pymongo.MongoClient(aws_resources['MONGO_CONNSTRING'])
db = client.rubisandbox
collection_name = aws_resources['MONGO_COLLECTION']
collection = db[collection_name]

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
    db[collection_name].find_one_and_update({'name': name}, {'$set': {'status': new_status}, '$addToSet': {'log' : new_log}}, upsert=True, return_document=pymongo.ReturnDocument.AFTER)

def validateJson(postData):
    #postData defined in app.py
	# has_planar_elems = False
	try:
		if "modelInformation" not in postData or "payload" not in postData:
			raise KeyError
		if "units" not in postData["modelInformation"] or "name" not in postData["modelInformation"]:
			raise MissingModelInfoKeysError
		# postData["modelInformation"]["name"] = timestampify(postData["modelInformation"]["name"])
		print("Model has necessary file metadata")
		updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has necessary file metadata")
		print("filename: " + postData["modelInformation"]["name"])
		fileTimeStamp = postData["modelInformation"]["name"][-15:]
		print(fileTimeStamp)
		timeStampRegEx = re.compile(r'\D_\d{6}_\d{6}')
		mo = timeStampRegEx.match(fileTimeStamp)
		if mo == None or fileTimeStamp[1] != '_' or fileTimeStamp[8] != '_':
			raise TimestampError
		print("Model has valid timestamp")
		updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has valid timestamp")
		valid_units = ["metric", "imperial"]
		if postData["modelInformation"]["units"] not in valid_units:
			raise InvalidUnitsError
		print("Model has valid units")
		updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has valid units")
		payload_elems = postData["payload"]
		planar_elements = []
		linear_elements = []
		for i in payload_elems:
			if "vertices" not in i.keys() or "metadata" not in i.keys():
				raise MissingPayloadKeysError
			print("Model has valid payload structure")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has valid payload structure")
			if not i["vertices"]:
			  	raise EmptyVerticesError
			print("Model has valid list of vertices")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has valid list of vertices")
			if not i["metadata"]:
			  	raise EmptyMetadataError
			print("Model has valid element metadata")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has valid element metadata")
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
			print("Model has a unique filename")
			updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has a unique filename")
		try:
			if len(planar_elements) > 0:
				postData["modelInformation"]["destination"] = settings['site-bucket']
				destination_bucket = faces_staging_bucket
				print("Model has been uploaded to staging area, waiting for tessellation")
				updateMongoStatus(postData["modelInformation"]["name"], 202, "Model has been uploaded to staging area, waiting for tessellation")
			else:
				destination_bucket = source_bucket
				print("Model has been successfully uploaded")
				updateMongoStatus(postData["modelInformation"]["name"], 200, "Model has been successfully uploaded")
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
		updateMongoStatus(postData["modelInformation"]["name"], 500, "error: timestamp is either missing or has been manually appended to file.")
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
