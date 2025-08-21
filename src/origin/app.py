import base64
import boto3
import botocore
from PIL import Image, ImageOps, ImageFile
from urllib import parse
import io
import logging
import uuid
import math

ImageFile.LOAD_TRUNCATED_IMAGES = True


logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.resource('s3')

BUCKET_NAME = 'xxxxxxxxxx'
V1_BUCKET_NAME = 'somewhere-public'

def resize_image_buffer(image_path):
    try:
        with Image.open(image_path) as original_image:
            image = ImageOps.exif_transpose(original_image) # ignore exif
            image.thumbnail(tuple(x / 2 for x in image.size))
            with io.BytesIO() as output:
                image.save(output, format=image.format)
                contents = output.getvalue()
                mimetype = image.get_format_mimetype()
                return contents, mimetype
    except Exception as e:
        logger.error(e)

def resize_and_crop(img_path, resize_width, resize_height, is_webp_requested, crop_type='middle'):
    try:

        size = (resize_width, resize_height)
        original_image = Image.open(img_path)
        mimetype = original_image.get_format_mimetype()
        img_format = '{}'.format(original_image.format)
        logger.info('Orignal img.format {}\n'.format(original_image.format))

        img = ImageOps.exif_transpose(original_image) # ignore exif
        
        
        
        
        logger.info('2')
        img_ratio = img.size[0] / float(img.size[1])
        ratio = size[0] / float(size[1])
        if ratio > img_ratio:
            y = int(size[0] * img.size[1] / img.size[0])
            logger.info('3')

            img = img.resize((size[0], y), Image.ANTIALIAS)
            logger.info('4')
            if crop_type == 'top':
                box = (0, 0, img.size[0], size[1])
            elif crop_type == 'middle':
                box = (0, (img.size[1] - size[1]) / 2,
                       img.size[0], (img.size[1] + size[1]) / 2)
            elif crop_type == 'bottom':
                box = (0, img.size[1] - size[1], img.size[0], img.size[1])
            else:
                raise ValueError('ERROR: invalid value for crop_type')
            img = img.crop(box)
        elif ratio < img_ratio:
            logger.info(
                "(size[0] = {} * img.size[0] = {}".format(size[0], img.size[0]))
            logger.info(
                "(size[1] = {} * img.size[1] = {}".format(size[1], img.size[1]))
            x = int(size[1] * img.size[0] / img.size[1])
            img = img.resize((x, size[1]), Image.ANTIALIAS)
            logger.info('333')

            if crop_type == 'top':
                box = (0, 0, size[0], img.size[1])
            elif crop_type == 'middle':
                box = ((img.size[0] - size[0]) / 2, 0,
                       (img.size[0] + size[0]) / 2, img.size[1])

            elif crop_type == 'bottom':
                box = (img.size[0] - size[0], 0, img.size[0], img.size[1])
            else:
                raise ValueError('ERROR: invalid value for crop_type')
            img = img.crop(box)
        else:
            img = img.resize((size[0], size[1]), Image.ANTIALIAS)
        with io.BytesIO() as output:
            if is_webp_requested:
                logger.info("resize to webp")
                img.save(output, format='webp', optimize=True, quality=100)
                mimetype = 'image/webp'
            else:
                if img_format == None or img_format == 'None':
                    img.save(output, format='png', optimize=True, quality=100)
                else:
                    img.save(output, format=img_format, optimize=True, quality=100)
            contents = output.getvalue()
            logger.info('resized format mimetype {}'.format(mimetype))
            return contents, mimetype

    except Exception as e:
        logger.error("============ PIL ERROR ===================")
        logger.error(e)
        raise Exception('PIL Error')


def _get_resize_info(uri):
    urldecode_uri = parse.unquote(uri, encoding='utf-8')
    is_webp_requested = False
    uri_elements = urldecode_uri.split('/')
    logger.info("_get_resize_info===============")
    logger.info(urldecode_uri)
    logger.info(uri_elements)
    webp = uri_elements[-2] if uri_elements[-2] == 'webp' else ''
    demention = uri_elements[-3] if webp == 'webp' else uri_elements[-2]
    orignal_uri_elements = [
        ele for ele in uri_elements if ele not in ['', webp, demention]]
    original_key = '/'.join(orignal_uri_elements)
    logger.info(original_key)
    logger.info(original_key.split('.')[-1])
    if original_key.split('.')[-1] == 'webp':
        original_key = original_key.replace('.webp', '')
        is_webp_requested = True
    urldecoded_original_key = parse.unquote(original_key)
    resize_width, resize_height = demention.split('x')
    logger.info("[[resize_width({}), resize_height({}), is_webp_requested({})]] ".format(
        resize_width, resize_height, is_webp_requested))
    logger.info("[[urldecoded_original_key({}), urldecode_uri({})]] ".format(
        urldecoded_original_key, urldecode_uri))
    return urldecoded_original_key, urldecode_uri, math.ceil(float(resize_width)), math.ceil(float(resize_height)), is_webp_requested


def _upload_to_s3(buffer, new_key, mimetype):
    s3.meta.client.upload_fileobj(io.BytesIO(buffer), BUCKET_NAME, new_key, ExtraArgs={
        'ContentType': mimetype, 'ACL': "public-read"})


def _upload_to_s3_v1(buffer, new_key, mimetype):
    s3.meta.client.upload_fileobj(io.BytesIO(buffer), V1_BUCKET_NAME, new_key, ExtraArgs={
        'ContentType': mimetype, 'ACL': "public-read"})


def check_and_resize_if_required_v1(event, context):
    logger.info("check_and_resize_if_required_v1=============")
    logger.info(event)
    request = event["Records"][0]["cf"]["request"]
    response = event["Records"][0]["cf"]["response"]
    request_uri = request['uri']
    logger.info('Request Uri@event  : {}\n'.format(request_uri))
    logger.info(
        'Event Response Status Code@event  : {}\n'.format(response['status']))
    if int(response['status']) in [403, 404]:  # not found
        original_key, decoded_uri, resize_width, resize_height, is_webp_requested = _get_resize_info(
            request_uri)
        logger.info('[[original_key]] {}\n'.format(original_key))
        try:
            s3.Object(V1_BUCKET_NAME, original_key).load() ############### v1
            logger.info('Start resize image')
            tmp_fname = original_key.replace('/', '')
            download_path = '/tmp/{}_{}'.format(uuid.uuid4(), tmp_fname)
            s3.meta.client.download_file(
                V1_BUCKET_NAME, original_key, download_path) ############### v1
            logger.info('download_file         {}'.format(tmp_fname))

            resized_buffer, mimetype = resize_and_crop(
                download_path, resize_width, resize_height, is_webp_requested)
            file_size = len(resized_buffer)
            logger.info('resize_image size {}'.format(file_size))

            new_key = decoded_uri[1:]

            _upload_to_s3_v1(resized_buffer, new_key, mimetype) ############### v1

            # lambda edge의 응답 용량은 1M 넘길수 없다. 이런 경우 리다이렉트
            if file_size > (1024*1024):
                urlencoded_new_key = parse.quote(new_key)
                response["status"] = 301
                response["headers"]["location"] = [
                    {"key": "Location", "value": f"/{urlencoded_new_key}"}]
            else:
                response['status'] = 200
                response['body'] = base64.b64encode(resized_buffer)
                response['bodyEncoding'] = 'base64'
                response['headers']['content-type'] = [
                    {'key': 'Content-Type', 'value': mimetype}]
                response['headers']['content-length'] = [
                    {'key': 'Content-Length', 'value': str(file_size)}]
            return response
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info(
                    '[[{}]] not exist. no file to resize'.format(original_key))
                # The object does not exist.
                return response
            else:
                logger.error(e)
                return response
        except Exception as e:
            logger.error(e)
    else:
        return response


def test_local(event,context): # LocalTestFunction
    import urllib.request as req 
    test_jpg_url =  "https://dx5onpmbc1hnr.cloudfront.net/v1/store/Omangosix/1d515b14-d3cc-4e27-aaf5-df14bb9595cd/2023_06_06_192653_Omangosix_Coffee___Dessert_food_2.JPG"
    test_jpg_url =  "https://dx5onpmbc1hnr.cloudfront.net/v1/store/sakura/2f591e27-4de6-47e5-9704-f9d85253b32a/2023_06_08_144157_SAKURA_food_1.jpg"
    test_jpg = "/tmp/test.jpg"
    req.urlretrieve(test_jpg_url, test_jpg)

    bytes_object, mimetype =  resize_and_crop(test_jpg, 100, 100, False)
    logger.info(mimetype)
    with open("output.jpeg", "wb") as f:
        f.write(bytes_object)

    logger.info("DONE")

def check_and_resize_if_required(event, context):
    logger.info(event)
    request = event["Records"][0]["cf"]["request"]
    response = event["Records"][0]["cf"]["response"]
    request_uri = request['uri']
    logger.info('Request Uri@event  : {}\n'.format(request_uri))
    logger.info(
        'Event Response Status Code@event  : {}\n'.format(response['status']))

    if int(response['status']) in [403, 404]:  # not found
        original_key, decoded_uri, resize_width, resize_height, is_webp_requested = _get_resize_info(
            request_uri)
        logger.info('[[original_key]] {}\n'.format(original_key))
        try:
            s3.Object(BUCKET_NAME, original_key).load()
            logger.info('Start resize image')
            tmp_fname = original_key.replace('/', '')
            download_path = '/tmp/{}_{}'.format(uuid.uuid4(), tmp_fname)
            s3.meta.client.download_file(
                BUCKET_NAME, original_key, download_path)
            logger.info('download_file         {}'.format(tmp_fname))

            resized_buffer, mimetype = resize_and_crop(
                download_path, resize_width, resize_height, is_webp_requested)
            file_size = len(resized_buffer)
            logger.info('resize_image size {}'.format(file_size))

            new_key = decoded_uri[1:]
            _upload_to_s3(resized_buffer, new_key, mimetype)

            # lambda edge의 응답 용량은 1M 넘길수 없다. 이런 경우 리다이렉트
            if file_size > (1024*1024):
                urlencoded_new_key = parse.quote(new_key)
                response["status"] = 301
                response["headers"]["location"] = [
                    {"key": "Location", "value": f"/{urlencoded_new_key}"}]
            else:
                response['status'] = 200
                response['body'] = base64.b64encode(resized_buffer)
                response['bodyEncoding'] = 'base64'
                response['headers']['content-type'] = [
                    {'key': 'Content-Type', 'value': mimetype}]
                response['headers']['content-length'] = [
                    {'key': 'Content-Length', 'value': str(file_size)}]
            return response
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info(
                    '[[{}]] not exist. no file to resize'.format(original_key))
                # The object does not exist.
                return response
            else:
                logger.error(e)
                return response
        except Exception as e:
            logger.error(e)
    else:
        return response
