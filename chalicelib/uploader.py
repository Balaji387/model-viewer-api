
from datetime import datetime
import json
import os
import boto3
from aws_settings import aws_resources
from botocore.client import Config

s3 = boto3.resource('s3')

settings = {
	'site-bucket': aws_resources['site-bucket'],
	'data-folder': aws_resources['data-folder']
}

mybucket = s3.Bucket(settings['site-bucket'])

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

def validateJson(postData):
    #postData defined in app.py
	try:
		#WrongNameError necessary to check that upload name matches name within JSON?
		if "modelInformation" not in postData or "payload" not in postData:
			raise KeyError
		if "units" not in postData["modelInformation"] or "name" not in postData["modelInformation"]:
			raise MissingModelInfoKeysError
		valid_units = ["metric", "imperial"]
		if postData["modelInformation"]["units"] not in valid_units:
			raise InvalidUnitsError
		payload_elems = postData["payload"]
		for i in payload_elems:
			if "vertices" not in i.keys() or "metadata" not in i.keys():
				raise MissingPayloadKeysError
			if not i["vertices"]:
			  	raise EmptyVerticesError
			if not i["metadata"]:
			  	raise EmptyMetadataError
		for obj in mybucket.objects.all():
			if (os.path.basename(obj.key)).lower() == (postData["modelInformation"]["name"] + ".json").lower():
				raise AlreadyInBucketError
		try:
			#process model info and create s3 tags for filtering purposes.
			tags = '&'.join([str(k)+"="+str(v) for k,v in postData["modelInformation"].iteritems()])
			if tags:
				mybucket.put_object(Key=settings["data-folder"] + "/" + postData["modelInformation"]["name"] + ".json", Body=json.dumps(postData), Tagging=tags)
			else:	
				mybucket.put_object(Key=settings["data-folder"] + "/" + postData["modelInformation"]["name"] + ".json", Body=json.dumps(postData))
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
