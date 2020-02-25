#!/usr/bin/python

import requests



#logic:
# 
#  parse URL, detect if:
# direct link or not
# if direct link, delete it
# if not, create a tnl.snapfish.com URL, follow it to imagizer
# translate imagizer url to direct link, then delete it
# 
# if direct link, parse the URL, figure out if openstack or S3
# run delete based on protocol
#
# 
with open('sample.csv', 'rU') as f:
  for line in f:
    print line
    ele = line.split(',')
    ele[-1] = ele[-1].strip('\n')
    print ele
    if (ele[0] == 'HIRES'):
      print 'hires'
    elif (ele[0] == 'LOWRES'):
      print 'lowres'
    elif (ele[0] == 'THUMBNAIL'):
      print 'thumbnail'
    else:
      print 'not doing anything'




#  sample data file
# type "help" for help
# HIRES,http://swiftbuckets1.sf-cdn.com/v1/snapfish/snapfish_uploads10949/hr/0533/449/438/0533449438018.jpg
# THUMBNAIL,SNAPFISH/4IXYfV5CEgRvp8Mzac3EIg/a/xfW_Ys6k7rpOztllFCXrWA/d/qhac9fJtVzxe-Z2ExgA-lg
# LOWRES,http://swiftbuckets1.sf-cdn.com/v1/snapfish/snapfish_uploads13445/lr/1661/936/001/UPLOAD/0533449438018lr.jpg
# HIRES,https://swiftbuckets7.sf-cdn.com/v1/uass/hires_8439/9225e09c-7f56-449c-9d60-d6c0169963de.jpg
# THUMBNAIL,SNAPFISH/1oqnHJNOFZzFQ3jP4hz1T-fnZWqfchaTRMgQwD_L7Po/a/brE2cz9pz_XfnEEoITWuwg/d/qhac9fJtVzxe-Z2ExgA-lg/time/YiAvj_R_jeDKhnkPBWQEag/h/s7/m/s7/l/cs
# HIRES,http://swiftbuckets1.sf-cdn.com/v1/snapfish/snapfish_uploads11137/hr/0533/922/318/0533922318018.jpg
# THUMBNAIL,SNAPFISH/QRbxil87T6L1WBAtNfr5vg/a/k7rRGUbL_czPSADqit0FEA/d/qhac9fJtVzxe-Z2ExgA-lg

# sample tnl call:
# https://tnl2.snapfish.com/assetrenderer/v2/thumbnail/SNAPFISH/1oqnHJNOFZzFQ3jP4hz1T-fnZWqfchaTRMgQwD_L7Po/a/brE2cz9pz_XfnEEoITWuwg/d/qhac9fJtVzxe-Z2ExgA-lg/time/YiAvj_R_jeDKhnkPBWQEag/h/s7/m/s7/l/cs
# get the 302 redirect, extract the Location:, translate the url, delete it.
# Location: http://irmw-cf.sf-cdn.com/snapfish/lowres_45592/brE2cz9pz_XfnEEoITWuwg_1oqnHJNOFZzFQ3jP4hz1T8y4H6Ukwe_s0J2lFeeFX9s.jpg?height=100&t=YiAvj_R_jeDKhnkPBWQEag
# 
