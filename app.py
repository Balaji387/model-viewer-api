from chalice import Chalice, NotFoundError, ChaliceViewError, UnauthorizedError, CognitoUserPoolAuthorizer
#from chalice import Chalice, NotFoundError, ChaliceViewError
from datetime import datetime
from chalicelib.uploader import validateJson
from chalicelib.aws_settings import aws_resources
import json
import os
import boto3
from botocore.client import Config

app = Chalice(app_name=aws_resources['app'])

s3 = boto3.resource('s3')

settings = {
	'site-bucket': aws_resources['site-bucket'],
	'data-folder': aws_resources['data-folder']
}

authorizer = CognitoUserPoolAuthorizer(aws_resources['userpool'], header='Authorization', provider_arns=aws_resources['arn'])

def parseS3Time(dt):
	return str(dt).split('+')[0]

def getModelNames(_bucket,_prefix):
	s3_objects_resp = s3.meta.client.list_objects(Bucket=_bucket, Prefix=_prefix)
	s3_objects = s3_objects_resp.get('Contents', [])
	while s3_objects_resp['IsTruncated']:
		s3_objects_resp = s3.meta.client.list_objects(Bucket=_bucket, Prefix=_prefix, Marker=_existing_s3_objects_resp["Contents"][-1]['Key'])
		s3_objects += s3_objects_resp.get('Contents', [])

	result = []
	for i in s3_objects:
		if not i['Key'] == _prefix:
			last_modified = parseS3Time(i['LastModified'])
			tags = {o['Key']: o['Value'] for o in s3.meta.client.get_object_tagging(Bucket = _bucket, Key = i['Key'])['TagSet']}
			tags['uploadTime'] = last_modified
			modelObj = {
				'model': i['Key'].replace(_prefix,"").replace(".json",""),
				's3_attributes': tags
			}
			result.append(modelObj)
	return result
		
	#return [i['Key'].replace(_prefix,"").replace(".json","") for i in s3_objects if not i['Key'] == _prefix]

# @app.route('/get-model-data/{modelname}', methods=['GET'], cors=True)
@app.route('/get-model-data/{modelname}', methods=['GET'], cors=True, authorizer=authorizer)
def getModelData(modelname):
	try:
		#print "in get model data"
		json_object = s3.Object(settings['site-bucket'],settings['data-folder']+"/" + modelname+".json")
		file_content = json_object.get()['Body'].read().decode('utf-8')
	except:
		raise NotFoundError(modelname)
	try:
		json_content = json.loads(file_content)
		uploadTime = parseS3Time(json_object.last_modified)
		print uploadTime
		json_content['modelInformation']['s3_attributes'] = {
			'uploadTime': uploadTime
		}
		return json.dumps(json_content)
	except:
		return ChaliceViewError()

# @app.route('/get-models', methods=['GET'], cors=True)
@app.route('/get-models', methods=['GET'], cors=True, authorizer=authorizer)
def getModels():
    try:
    	result = json.dumps(getModelNames(settings['site-bucket'],settings['data-folder']+"/"))
    	return result
    except:
    	raise ChaliceViewError()

# @app.route('/post-model', methods=['POST'], cors=True)
@app.route('/post-model', methods=['POST'], cors=True, authorizer=authorizer)
def postModel():
	postData = app.current_request.json_body
	result = validateJson(postData)
	return json.dumps(result)