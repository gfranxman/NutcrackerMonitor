#! /usr/bin/env python

'''
    since nutcracker's status structure doesnt separate its backends from 
    its other configuration details we'll assume those with colons are backends
    we do some trickery with the keys:
        UPPERCASE keys are pool names # our own convention
        lowercase keys are attributes # our own convention
        keys:with:colons are backend servers
    maybe I should submit a patch that includes a status version number and fixes this to nutcracker
'''

import argparse # python2.7+
import socket
import json
from pprint import pprint

# parse the command line
parser = argparse.ArgumentParser(description='ballgazer -- a TwemProxy/Nutcracker monitor')
parser.add_argument("server_addr", help="the nutcracker server hostname",)
parser.add_argument('--port', action='store',dest='server_port', help='the nutcracker port', default=22222, type=int )
parser.add_argument("pools", metavar='poolname',  nargs='*', help="one or more pool names, empty to list them")

args = parser.parse_args()


# connect and parse the results
conn = socket.create_connection( (args.server_addr, args.server_port) )
buf = True
content = ''
while buf:
    buf = conn.recv( 1024*100 )
    content += buf
conn.close()

data = json.loads( content )

def display_pool_list( title, keys ):
    report_title = title + ' (backends/connections/server_ejections)'
    print report_title
    print "=" * len( report_title )
    for k in sorted( keys ):
        client_connections = data[k]['client_connections']
        server_ejects = data[k]['server_ejects']
        num_of_backends = 0
        footnote = ''

        for bk in data[k].keys():
            if ":" in bk:
                num_of_backends += 1

       
        if num_of_backends == 0:
            footnote += '*'
        if server_ejects > 0:
            footnote += '!'
        print "%25s ( %d/%d/%d ) %s" % ( 
            k, 
            num_of_backends, client_connections, server_ejects, 
            footnote,
        )

    print "\n\n"

# display
if not args.pools:
    # print the server's stats
    addr_str = "%s:%d" % ( args.server_addr, args.server_port )
    print addr_str
    print "=" * len( addr_str )
    for k in data.keys():
        #if k[0] == k[0].lower():
        #if data[k][0] != '{':
        try:
            data[k]['server_ejects']
            pass # it's a backend server

        except TypeError, its_a_struct_or_int:
            print "%10s : %s" % ( k, data[k] )
            
        except KeyError, its_a_server_stat:
            print "%10s : %s" % ( k, data[k] )
    print "\n"

    # display the available pools
    active_pools = []
    inactive_pools = []
    broken_pools = []
    for k in sorted( data.keys() ):
        if k[0] == k[0].upper():
            client_connections = data[k]['client_connections']
            server_ejects = data[k]['server_ejects']
            num_of_backends = 0

            for bk in data[k].keys():
                if ":" in bk:
                    num_of_backends += 1


            if client_connections == 0:
                inactive_pools.append( k )
            else:
                if  server_ejects > 0:
                    broken_pools.append( k )
                elif num_of_backends == 0:
                    broken_pools.append( k )
                else:
                    active_pools.append( k )

    display_pool_list( "Broken", broken_pools ) 
    display_pool_list( "Active", active_pools ) 
    display_pool_list( "Unused", inactive_pools ) 

else:
    for pool in args.pools:
        data[pool][u'pool_name'] = pool # inject the poolname so the result looks like valid json
        print( json.dumps( data[pool], indent=4)  )

