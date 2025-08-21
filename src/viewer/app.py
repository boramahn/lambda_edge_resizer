from urllib import parse
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import math
# DEFAULT_DEMENTION={'w':1600, 'h':1000}

# width / height 
def manipulate_request(event, context):
    try:


        logger.info(event)
        request = event['Records'][0]['cf']['request']
        query_string = request['querystring']        
        query_string_dict = parse.parse_qs(query_string)
        logger.info('[[dict query_string]] : {}\n'.format(query_string_dict))
        if not ('w' in query_string_dict and 'h' in query_string_dict):
            logger.info("[PASS]There is No Resizing request(w,h)")
            return request # resize요청 없으면 그냥 넘긴다. (w,h 모두있는경우만)
            
        width = query_string_dict.get('w')[0]
        height = query_string_dict.get('h')[0]

        try:
            width = ''.join(width)
            height = ''.join(height)
            width = math.ceil(float(width))
            height = math.ceil(float(height))
        except ValueError:
            logger.warn('ValueError width/height {} {}'.format(width, height))
            return request # width/ height가 int형태가 아니면 원본
        
        try:
            accept_values = request['headers']['accept'][0]['value']
            is_webp_supported = 'webp' in accept_values
        except:
            is_webp_supported = False
        if 'webp' in query_string_dict.keys(): # 클라이언트에서 webp변환을 거부하는 경우
            webp = query_string_dict.get('webp')[0]
            if webp.upper() == 'NO':
                logger.info('NO webp requested\n')
                is_webp_supported = False

        uri = request['uri']
        uri_elements = uri.split('/')
        uri_elements.insert(-1, '{}x{}'.format(width,height))
        # uri "/owner/raymond/8e0dc3f3-8f75-457f-8d57-663fe0512c99/2022_03_16_144449_help.qookly%2Braymond%40gmail.com_food_0.jpeg"
        # new_uri "/owner/raymond/8e0dc3f3-8f75-457f-8d57-663fe0512c99/22x888/2022_03_16_144449_help.qookly%2Braymond%40gmail.com_food_0.jpeg.webp"
        new_uri = '/'.join(s.strip('/') for s in uri_elements)
        if is_webp_supported : new_uri = '{}.webp'.format(new_uri)

        logger.info('[[new_uri]] {}\n'.format(new_uri))
        request['uri'] = new_uri

        logger.info(request)
        return request
    except Exception as e:
        logger.error(e)
