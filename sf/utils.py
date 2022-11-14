
import base64
import configparser
import datetime
import hashlib
import hmac
import json
import logging
import pytz
import re
import requests
import sys
import time
import urllib.parse

class ObjectStorage:
  def __init__ (self):
    # Example file:
    # http://swiftbuckets.sf-cdn.com/v1/encoding/migrationHosting_5002/lr_0159_191_555_0159191555012lr.jpg

    # dict of lists, store user, tenant, auth token, password, requests session:
    # {'cluster_tenant': ['cluster', 'tenant', 'user-str', 'password', 'session-obj','auth-token']
    
    config = configparser.ConfigParser()
    config.read("/home/ccarrigan/git/storage_scripts/conf/setup.ini")

    # s1.sf-cdn.com snapfish
    # s1.sf-cdn.com uass

    # s2.sf-cdn.com snapfish
    # s2.sf-cdn.com uass

    # swiftbuckets.sf-cdn.com encoding
    # swiftbuckets.sf-cdn.com snapfish
    # swiftbuckets.sf-cdn.com uass

    self.session = {
        'swiftbuckets-encoding': ['swiftbuckets', 'encoding', config['swiftbuckets-encoding']['user'], config['swiftbuckets-encoding']['password'], '', ''],
        'swiftbuckets-snapfish': ['swiftbuckets', 'snapfish', config['swiftbuckets-snapfish']['user'], config['swiftbuckets-snapfish']['password'], '', ''],
        'swiftbuckets-uass':     ['swiftbuckets', 'uass', config['swiftbuckets-uass']['user'], config['swiftbuckets-uass']['password'], '', ''],
        's1-snapfish':           ['s1', 'snapfish', config['s1-snapfish']['user'], config['s1-snapfish']['password'], '', ''],
        's1-uass':               ['s1', 'uass', config['s1-uass']['user'], config['s1-uass']['password'], '', ''],
        's2-snapfish':           ['s2', 'snapfish', config['s2-snapfish']['user'], config['s2-snapfish']['password'], '', ''],
        's2-uass':               ['s2', 'uass', config['s2-uass']['user'], config['s2-uass']['password'], '', '']
        }

    self.create_openstack_session()

    self.tnl = 'http://tnl.snapfish.com/assetrenderer/v2/thumbnail/'
    self.tnl_sess = requests.session()

#        sfCellNameMap.put("dm60000", new DFSMapBean("swiftbuckets.sf-cdn.com", "snapfish"));
#        sfCellNameMap.put("dm60002", new DFSMapBean("swiftbuckets.sf-cdn.com", "encoding"));
#        sfCellNameMap.put("dm80001", new DFSMapBean("swiftbuckets.sf-cdn.com", "uass"));
#        sfCellNameMap.put("ldm60000", new DFSMapBean("swiftbuckets.sf-cdn.com", "snapfish"));
#        sfCellNameMap.put("ldm60002", new DFSMapBean("swiftbuckets.sf-cdn.com", "encoding"));
#        sfCellNameMap.put("ldm80001", new DFSMapBean("swiftbuckets.sf-cdn.com", "uass"));

#        sfCellNameMap.put("dm70000", new DFSMapBean("s1.sf-cdn.com", "snapfish"));
#        sfCellNameMap.put("dm80002", new DFSMapBean("s1.sf-cdn.com", "uass"));
#        sfCellNameMap.put("ldm70000", new DFSMapBean("s1.sf-cdn.com", "snapfish"));
#        sfCellNameMap.put("ldm80002", new DFSMapBean("s1.sf-cdn.com", "uass"));

  def clean_up_url(self, url):
    url = url.replace('"','"')
    url = url.replace('https://','http://')
    url = url.replace('swiftbuckets1.sf-cdn.com','s1.sf-cdn.com')
    url = url.replace('swiftbuckets3.sf-cdn.com','s1.sf-cdn.com')
    url = url.replace('swiftbuckets4.sf-cdn.com','s2.sf-cdn.com')
    url = url.replace('swiftbuckets7.sf-cdn.com','s2.sf-cdn.com')
    url = url.replace('esthreecos-ak.sf-cdn.com','esthreecos1.sf-cdn.com')

    url = url.replace('images1.snapfish.com/dm60000','swiftbuckets.sf-cdn.com/v1/snapfish')
    url = url.replace('images1.snapfish.com/dm60002','swiftbuckets.sf-cdn.com/v1/encoding')
    url = url.replace('images1.snapfish.com/dm80001','swiftbuckets.sf-cdn.com/v1/uass')
    url = url.replace('images1.snapfish.com/ldm60000','swiftbuckets.sf-cdn.com/v1/snapfish')
    url = url.replace('images1.snapfish.com/ldm60002','swiftbuckets.sf-cdn.com/v1/encoding')
    url = url.replace('images1.snapfish.com/ldm80001','swiftbuckets.sf-cdn.com/v1/uass')

    url = url.replace('images2.snapfish.com/dm60000','swiftbuckets.sf-cdn.com/v1/snapfish')
    url = url.replace('images2.snapfish.com/dm60002','swiftbuckets.sf-cdn.com/v1/encoding')
    url = url.replace('images2.snapfish.com/dm80001','swiftbuckets.sf-cdn.com/v1/uass')
    url = url.replace('images2.snapfish.com/ldm60000','swiftbuckets.sf-cdn.com/v1/snapfish')
    url = url.replace('images2.snapfish.com/ldm60002','swiftbuckets.sf-cdn.com/v1/encoding')
    url = url.replace('images2.snapfish.com/ldm80001','swiftbuckets.sf-cdn.com/v1/uass')

    url = url.replace('images1.snapfish.com/dm70000','s1.sf-cdn.com/v1/snapfish')
    url = url.replace('images1.snapfish.com/dm80002','s1.sf-cdn.com/v1/uass')
    url = url.replace('images1.snapfish.com/ldm70000','s1.sf-cdn.com/v1/snapfish')
    url = url.replace('images1.snapfish.com/ldm80002','s1.sf-cdn.com/v1/uass')

    url = url.replace('images2.snapfish.com/dm70000','s1.sf-cdn.com/v1/snapfish')
    url = url.replace('images2.snapfish.com/dm80002','s1.sf-cdn.com/v1/uass')
    url = url.replace('images2.snapfish.com/ldm70000','s1.sf-cdn.com/v1/snapfish')
    url = url.replace('images2.snapfish.com/ldm80002','s1.sf-cdn.com/v1/uass')

    url = url.replace('irmw2.sf-cdn.com','swiftbuckets.sf-cdn.com')
    url = url.replace('irmw-cf.sf-cdn.com','esthreecos1.sf-cdn.com')
    url = url.replace('irmw-s1.sf-cdn.com','s1.sf-cdn.com')
    url = url.replace('irmw-s2.sf-cdn.com','s2.sf-cdn.com')
    url = url.rstrip()
    url_pattern = re.compile(r"(http://.*.sf-cdn.com/.*(\.jpg|\.mp4|\.mpg))(\?.*)$")
    m = url_pattern.match(url)
    if m:
      url = m.group(1)
    return url

  def create_openstack_session(self, cluster_tenant='all'):
    logging.info("Creating session for " + cluster_tenant)
    if cluster_tenant == 'all':
      for key in self.session:
        logging.debug(self.session[key][0])
        self.session[key][4] = requests.session()
        try: 
          temp_resp = self.session[key][4].get('http://'+ self.session[key][0] + '.sf-cdn.com/auth/v1.0', 
            headers={"X-Auth-User": self.session[key][2], "X-Auth-Key": self.session[key][3] })
        except urllib3.connection.HTTPConnection:
          print(f"Failed to get auth token, exiting. work/active/{data_file}")
          logging.critical(f"Failed to get auth token, exiting. work/active/{data_file}")
          sys.exit()
        self.session[key][5] = temp_resp.headers["X-Auth-Token"]
        logging.debug(self.session[key])
    else:
      self.session[cluster_tenant][4] = requests.session()
      try: 
        temp_resp = self.session[cluster_tenant][4].get('http://'+ self.session[cluster_tenant][0] + '.sf-cdn.com/auth/v1.0', 
          headers={"X-Auth-User": self.session[cluster_tenant][2], "X-Auth-Key": self.session[cluster_tenant][3] })
      except urllib3.connection.HTTPConnection:
        print(f"Failed to get auth token, exiting. work/active/{data_file}")
        logging.critical(f"Failed to get auth token, exiting. work/active/{data_file}")
        sys.exit()
      self.session[cluster_tenant][5] = temp_resp.headers["X-Auth-Token"]
    logging.info("Done creating session for " + cluster_tenant)

  def delete_object_file(self, url):
    # extract server and URL pattern
    # pass to either cos or openstack methods for delete, validate it ends in '.jpg', '.mp4', or '.mpg'
    # return deletion status, pass status back through
    host = ''
    host_pattern = re.compile(r"http://(.*.sf-cdn.com)/(.*)(\.jpg|\.mp4|\.mpg)$")
    m = host_pattern.match(url)
    if m:
      host = m.group(1)

    if host and host == 'esthreecos1.sf-cdn.com':
      return delete_cos_object(url)
    elif host and (host == 'swiftbuckets.sf-cdn.com' 
                  or host == 's1.sf-cdn.com' 
                  or host == 's2.sf-cdn.com'): 
      return self.delete_openstack_object(url)
    else:
      logging.info("Server or URL pattern didn't match: " + url)
      return False

  # assume same url, same account for batch delete
  def delete_openstack_batch(self, list_urls):
    # use a regex to fix a problem, get two problems
    url_pattern = re.compile(r"http://([^.]+).sf-cdn.com/v1/([^/]+)/([^/]+)/(.*)$")
    # test just the first entry
    try:
      m = url_pattern.match(list_urls[0])
    except IndexError:
      print(f"Exiting. Failed to match pattern on {list_urls}, working on file: {data_file}")
      logging.critical(f"Exiting. Failed to match pattern on {list_urls}, working on file: {data_file}")
      sys.exit()
    # extract container and object into an array
    delete_list =''

    if m:
      cluster = m.group(1)
      tenant = m.group(2)
      container = m.group(3)
      obj = m.group(4)
      logging.warning(f"cluster: {cluster}, tenant: {tenant}, 1st container: {container}, 1st object: {obj}")
      cluster_tenant = cluster + '-' + tenant

      delete_url = f"http://{cluster}.sf-cdn.com/v1/{tenant}?bulk-delete=true"

      for url in list_urls:
        url_match = url_pattern.match(url)
        container = url_match.group(3)
        obj = url_match.group(4)
        # url_pattern.extract(url)
        logging.debug(f"URL to delete: {container}/{obj}")
        delete_list += urllib.parse.quote(f"{container}/{obj}")+'\n'
      
      try:
        logging.debug(self.session[cluster_tenant])
        logging.debug(f"about to delete: {delete_list}")
        start = time.perf_counter()
        r = self.session[cluster_tenant][4].post(delete_url, data=delete_list, headers={'Content-Type':'text/plain', 'X-Auth-Token': self.session[cluster_tenant][5], 'Accept': 'application/json'})
        request_time = time.perf_counter() - start
        logging.info(r)
        logging.info(r.json())
        r_json = r.json()
        logging.info("json response: " + json.dumps(r.json(), indent=1))
        logging.warning(f"Attempted to delete {len(list_urls)} objects, actually deleted: {r_json['Number Deleted']} in {request_time} total seconds or {request_time/len(list_urls)} seconds per object (total_time/total_files)")
        logging.info(f"Status: '{r_json['Response Status']}', Number deleted: {r_json['Number Deleted']}, Number Not Found: {r_json['Number Not Found']}")
        if r_json['Response Status'] == '400 Bad Request' and r_json['Errors'][0][0][1] == '401 Unauthorized':
          raise requests.HTTPError()
        # 19-Oct-22 18:42:15.019 - DEBUG: {'Response Status':
        # '200 OK', 'Response Body': '', 'Number Deleted': 0, 'Number Not
        # Found': 1000, 'Errors': []}
        r.raise_for_status()
        return True
      except requests.HTTPError:
        logging.info("status code: " + str(r.status_code) + " for URL: " + url)
        if r.status_code == 401 or (r_json['Response Status'] == '400 Bad Request' and r_json['Errors'][0][0][1] == '401 Unauthorized'):
          logging.warning("Got HTTP 401, reauth and try delete one time more for " + url)
          self.create_openstack_session(cluster_tenant)
          r = self.session[cluster_tenant][4].delete(url, headers={'X-Auth-Token': self.session[cluster_tenant][5]})
        return False
      except requests.exceptions.ConnectionError:
        # set up the connection again
        logging.warning("Got ConnectionError, reauth and try delete one time more for " + url)
        self.create_openstack_session(cluster_tenant)
        r = self.session[cluster_tenant][4].delete(url, headers={'X-Auth-Token': self.session[cluster_tenant][5]})
        return False
      except KeyError:
        logging.critical(f"Exiting. No key for: {cluster_tenant}")
        sys.exit()
    else:
      logging.info('URL does not match expected pattern: ' + url)
      return False
    return True

  def delete_openstack_object(self, url):
    # use a regex to fix a problem, get two problems
    url_pattern = re.compile(r"http://([^.]+).sf-cdn.com/v1/([^/]+)/([^/]+)/(.*)$")
    m = url_pattern.match(url)
    if m:
      cluster = m.group(1)
      logging.info("cluster: " + cluster)
      tenant = m.group(2)
      logging.info("tenant: " + tenant)
      container = m.group(3)
      logging.info("container: " + container)
      obj = m.group(4)
      logging.info("object: " + obj)
      cluster_tenant = cluster + '-' + tenant
      try:
        logging.debug(self.session[cluster_tenant])
        r = self.session[cluster_tenant][4].delete(url, headers={'X-Auth-Token': self.session[cluster_tenant][5]})
        logging.debug(r)
        r.raise_for_status()
        return True
      except requests.HTTPError:
        logging.info("status code: " + str(r.status_code) + " for URL: " + url)
        if r.status_code == 401:
          logging.warning("Got HTTP 401, reauth and try delete one time more for " + url)
          self.create_openstack_session(cluster_tenant)
          r = self.session[cluster_tenant][4].delete(url, headers={'X-Auth-Token': self.session[cluster_tenant][5]})
        return False
      except requests.exceptions.ConnectionError:
        # set up the connection again
        logging.warning("Got ConnectionError, reauth and try delete one time more for " + url)
        self.create_openstack_session(cluster_tenant)
        r = self.session[cluster_tenant][4].delete(url, headers={'X-Auth-Token': self.session[cluster_tenant][5]})
        return False
      except KeyError:
        logging.critical(f"Exiting. No key for: {cluster_tenant}")
        sys.exit()
    else:
      logging.info(f"URL does not match expected pattern: {url}")
      return False
    # need to return boolean

  def get_referer(self, url):
    url = self.tnl + url.strip()
    tnl_resp = self.tnl_sess.head(url)

    logging.info(url)
    
    if tnl_resp.status_code == 302:
      # grab the 'Location: header, put in log
      logging.warning(str(tnl_resp.status_code) + ' status Location: ' + tnl_resp.headers['Location'] + " for: " + url)
      # tnl_f.write(tnl_resp.headers['Location'] + '\n')
      return tnl_resp.headers['Location']

    elif tnl_resp.status_code == 404:
      logging.debug(str(tnl_resp.status_code) + ' status, ignore for: ' + url)
      # do nothing:
      return False
    else:
      logging.error(str(tnl_resp.status_code) + ' status, not handled for: ' + url)
      # should we log this to a failure file?
      # but do nothing:
      return ''

def make_digest(message, key):
  logging.debug("key: " + key)
  key = bytes(key, 'UTF-8')
  message = bytes(message, 'UTF-8')
    
  digester = hmac.new(key, message, hashlib.sha1)
  signature1 = digester.digest()
  logging.debug("Signature 1: " + str(signature1))
    
  signature2 = base64.urlsafe_b64encode(signature1)    
  logging.debug("Signature 2: " + str(signature2))
    
  return str(signature2, 'UTF-8')
  
# defaults to phl - esthreecos1.sf-cdn.com
def delete_cos_object(object_file, host='esthreecos1.sf-cdn.com', bucket='snapfish'):
  s3_host = 'esthreecos1.sf-cdn.com'
  s3_key='tiEHBOJaJLFC0MDwfs5k'
  s3_secret='hNnKKEdpZ8IKfFFsMQgcNFfKAdynoTh5PndRzJ8l'
  s3_session = requests.session()

  # match on: http://irmw-cf.sf-cdn.com/snapfish/lowres_7959/nJprn-UfyjIACAzzU0sLDMk-bPJGXBiJWFSqmOxeveo.jpg
  p = re.compile(r"http://(.*.sf-cdn.com)/([^/]+)/(.*(\.jpg|\.mp4|\.mpg))$")
  m = p.match(object_file)
  if m:
    logging.debug("Pattern for pulling out host, bucket, object_file matched!")
    host = m.group(1)
    bucket = m.group(2)
    object_file = m.group(3)
  
  logging.info("Object to delete - host: '" + host + "' bucket: '" + bucket + "' object_file: '" + object_file)
  pst_timezone = pytz.timezone("US/Pacific")
  date_value = pst_timezone.localize(datetime.datetime.now()).strftime('%a, %d %b %Y %H:%M:%S %z')
  # print(date_value)
  content_type = "image/jpeg"
  resource = "/" + bucket + "/" + object_file
  to_sign = "DELETE\n\n" + content_type + "\n" + date_value + "\n" + resource
  logging.debug("Request header to sign: " + to_sign)
  signed = make_digest(to_sign, s3_secret)
  logging.debug("Signed request header: " + signed)
  headers = {"Host": s3_host, "Date": date_value, "Content-Type": content_type, "Authorization": "AWS " + s3_key + ":" + signed}
  logging.debug("Request headers: " + str(headers))
  s3_resp = s3_session.delete("http://" + host + resource, headers=headers)
  logging.debug(str(s3_resp))
  logging.debug(str(s3_resp.headers))
  if s3_resp.status_code < 400:
    return True
  else:
    return False
