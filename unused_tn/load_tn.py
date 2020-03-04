#!/usr/bin/python

import stomp
# import arg 



# conn = stomp.Connection11('mq-test.tnt6-zone1.aus2')
# conn = stomp.Connection10()

conn = stomp.Connection11([('mq-test.tnt6-zone1.aus2', 61613)])
# conn = stomp.Connection11([('127.0.0.1', 61613)])

# conn.set_listener("testlistener", TestListener("123", print_to_log=True))
  
conn.start()
   
conn.connect()
    
conn.send('CassQueue', 'SimplesAssim')
     
conn.disconnect()
