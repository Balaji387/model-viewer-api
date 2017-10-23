
from datetime import datetime
import json
import os
import boto3
from aws_settings import aws_resources
from botocore.client import Config

s3 = boto3.resource('s3')

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

def timestampify(s):
	t = datetime.now().strftime('%y%m%d_%H%M%S')
	return s + "_" + t

def validateJson(postData):
    #postData defined in app.py
	try:
		#WrongNameError necessary to check that upload name matches name within JSON?
		if "modelInformation" not in postData or "payload" not in postData:
			raise KeyError
		if "units" not in postData["modelInformation"] or "name" not in postData["modelInformation"]:
			raise MissingModelInfoKeysError
		postData["modelInformation"]["name"] = timestampify(postData["modelInformation"]["name"])
		valid_units = ["metric", "imperial"]
		if postData["modelInformation"]["units"] not in valid_units:
			raise InvalidUnitsError
		payload_elems = postData["payload"]
		planar_elements = []
		linear_elements = []
		for i in payload_elems:
			if "vertices" not in i.keys() or "metadata" not in i.keys():
				raise MissingPayloadKeysError
			if not i["vertices"]:
			  	raise EmptyVerticesError
			if not i["metadata"]:
			  	raise EmptyMetadataError
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
		try:
			if len(planar_elements) > 0:
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
		return {"success": True}
	except KeyError:
		return {"error": "'modelInformation' and 'payload' properties are required"}
	except MissingModelInfoKeysError:
		return {"error": "'modelInformation' must contain 'units' and 'name'"}
	except InvalidUnitsError:
	  	return {"error": "'units' values must be either 'imperial' or 'metric'"}
	except MissingPayloadKeysError:
	  	return {"error": "'payload' must include 'vertices' and 'metadata' for each element"}
	except EmptyVerticesError:
	  	return {"error": "'vertices' data is empty"}
	except EmptyMetadataError:
	  	return {"error": "'metadata' field is empty"}
	except AlreadyInBucketError:
		return {"error": "File with this name already exists in S3 bucket. Please rename file and try again." }
	except:
		return {"error": "unknown error occurred"}
