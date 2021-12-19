import io
import boto3
import base64
from PIL import Image
import requests as req
import configparser

def get_images(b64, config):
  folder_id = "b1grguml21m8dm6rbrmi"
  data = '{"folderId": "'+ folder_id +'", "analyzeSpecs": [{"features": [{"type": "FACE_DETECTION"}],"content":"' + b64.decode('utf-8') + '"}]}'
  config.read('/function/code/config/keys.ini')
  secret_key = config['yacloud']['api_key']
  url = 'https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze'
  headers = {
    'Authorization': "Api-Key {}".format(secret_key),
    'Content-Type': "application/json"
  }
  r = req.post(url, headers = headers, data = data)
  detected = r.json()['results'][0]['results'][0]['faceDetection'] != {}
  if detected:
    faces = r.json()['results'][0]['results'][0]['faceDetection']['faces']
    return faces
  else:
    return []

def get_storage(config):
    config.read('/function/code/config/config')
    region = config['default']['region']
    config.read('/function/code/config/keys.ini')
    key_id = config['aws']['api_id']
    secret_key = config['aws']['api_key']
    session = boto3.session.Session()
    s3resource = session.resource(
      service_name='s3',
      endpoint_url='https://storage.yandexcloud.net',
      aws_access_key_id = key_id,
      aws_secret_access_key = secret_key,
      region_name=region
    )
    return s3resource

def upload_images(images, bucket, key):
  names = []
  for i in range(len(images)):
    image = images[i]
    image.save('/tmp/image.jpg')
    name = '__cropped_imgs/{image_name}/image{num}{ext}'.format(image_name=key, num=i+1, ext='.image')
    names.append(name)
    with open('/tmp/image.jpg', 'rb') as image_file:
      bucket.upload_fileobj(image_file, name)
  return names

def get_mq(config):
  config.read('/function/code/config/config')
  region = config['default']['region']
  config.read('/function/code/config/keys.ini')
  key_id = config['aws']['api_id']
  secret_key = config['aws']['api_key']
  session = boto3.session.Session()
  sqs = session.resource(
    service_name='sqs',
    endpoint_url='https://message-queue.api.cloud.yandex.net',
    aws_access_key_id = key_id,
    aws_secret_access_key = secret_key,
    region_name=region
  )
  return sqs

def crop_images(faces, data):
  stream = io.BytesIO(data)
  im = Image.open(stream)
  boxes = []
  for i in range(len(faces)):
    face = faces[i]
    position = face['boundingBox']['vertices']
    box = (int(position[0]['x']), int(position[0]['y']), int(position[2]['x']), int(position[2]['y']))
    boxes.append(box)
  images = []    
  for i in range(len(boxes)):
    image = im.crop(boxes[i])
    images.append(image)
  return images

def send_names_to_queue(sqs, names, key):
  q = sqs.get_queue_by_name(QueueName = 'images-queue')
  q.send_message(MessageBody = 'Images from {}'.format(key),
    MessageAttributes={
    'string': {
      'StringValue': str(names),
      'DataType': 'string'
    }
  })

def handler(event, context):    
  config = configparser.ConfigParser()
  bucket_name = event['messages'][0]['details']['bucket_id']
  object_name = event['messages'][0]['details']['object_id']
  if object_name.startswith("__cropped_imgs/"):
    return {
      'statusCode': 201
    }
  result = get_storage(config)
  object = result.ObjectSummary(bucket_name, object_name)
  data = object.get()['Body'].read()
  faces = get_images(base64.b64encode(data), config)
  if faces != []:
    bucket = result.Bucket(bucket_name)
    images = crop_images(faces, data)
    names = upload_images(images, bucket, object_name)
    send_names_to_queue(get_mq(config), names, object_name)
  return {
    'statusCode': 201
  }
