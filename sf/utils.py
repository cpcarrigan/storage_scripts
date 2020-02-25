
import base64
import configparser
import datetime
import hashlib
import hmac
import logging
import pytz
import re
import requests
import sys

class ObjectStorage:
  def __init__ (self):
    # Example file:
    # http://swiftbuckets.sf-cdn.com/v1/encoding/migrationHosting_5002/lr_0159_191_555_0159191555012lr.jpg

    # dict of lists, store user, tenant, auth token, password, requests session:
    # {'cluster_tenant': ['cluster', 'tenant', 'user-str', 'password', 'session-obj','auth-token']
    
    config = configparser.ConfigParser()
    config.read("../conf/setup.ini")

    self.session = {
        'swiftbuckets-uass':     ['swiftbuckets', 'uass', config['swiftbuckets-uass']['user'], config['swiftbuckets-uass']['password'], '', ''],
        'swiftbuckets-snapfish': ['swiftbuckets', 'snapfish', config['swiftbuckets-snapfish']['user'], config['swiftbuckets-snapfish']['password'], '', ''],
        'swiftbuckets-encoding': ['swiftbuckets', 'encoding', config['swiftbuckets-encoding']['user'], config['swiftbuckets-encoding']['password'], '', ''],
        's1-snapfish':           ['s1', 'snapfish', config['s1-snapfish']['user'], config['s1-snapfish']['password'], '', ''],
        's1-uass':               ['s1', 'uass', config['s1-uass']['user'], config['s1-uass']['password'], '', ''],
        's2-snapfish':           ['s2', 'snapfish', config['s2-snapfish']['user'], config['s2-snapfish']['password'], '', ''],
        's2-uass':               ['s2', 'uass', config['s2-uass']['user'], config['s2-uass']['password'], '', '']
        }

    self.create_openstack_session()

    self.tnl = 'http://tnl.snapfish.com/assetrenderer/v2/thumbnail/'
    self.tnl_sess = requests.session()

  def clean_up_url(self, url):
    url = url.replace('https://','http://')
    url = url.replace('swiftbuckets1.sf-cdn.com','s1.sf-cdn.com')
    url = url.replace('swiftbuckets3.sf-cdn.com','s1.sf-cdn.com')
    url = url.replace('swiftbuckets4.sf-cdn.com','s2.sf-cdn.com')
    url = url.replace('swiftbuckets7.sf-cdn.com','s1.sf-cdn.com')
    url = url.replace('esthreecos-ak.sf-cdn.com','esthreecos1.sf-cdn.com')
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
        temp_resp = self.session[key][4].get('http://'+ self.session[key][0] + '.sf-cdn.com/auth/v1.0', 
          headers={"X-Auth-User": self.session[key][2], "X-Auth-Key": self.session[key][3] })
        self.session[key][5] = temp_resp.headers["X-Auth-Token"]
        logging.debug(self.session[key])
    else:
      self.session[cluster_tenant][4] = requests.session()
      temp_resp = self.session[cluster_tenant][4].get('http://'+ self.session[cluster_tenant][0] + '.sf-cdn.com/auth/v1.0', 
        headers={"X-Auth-User": self.session[cluster_tenant][2], "X-Auth-Key": self.session[cluster_tenant][3] })
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
          logging.warn("Got HTTP 401, reauth and try delete one time more for " + url)
          self.create_openstack_session(cluster_tenant)
          r = self.session[cluster_tenant][4].delete(url, headers={'X-Auth-Token': self.session[cluster_tenant][5]})
        return False
      except requests.exceptions.ConnectionError:
        # set up the connection again
        logging.warn("Got ConnectionError, reauth and try delete one time more for " + url)
        self.create_openstack_session(cluster_tenant)
        r = self.session[cluster_tenant][4].delete(url, headers={'X-Auth-Token': self.session[cluster_tenant][5]})
        return False
      except KeyError:
        logging.error("No key for: " + cluster_tenant)
        sys.exit()
    else:
      logging.info('URL does not match expected pattern: ' + url)
      return False
    #
    # need to return boolean
  def get_referer(self, url):
    url = self.tnl + url.strip()
    tnl_resp = self.tnl_sess.head(url)

    logging.info(url)
    
    if tnl_resp.status_code == 302:
      # grab the 'Location: header, put in log
      logging.warn(str(tnl_resp.status_code) + ' status Location: ' + tnl_resp.headers['Location'] + " for: " + url)
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
