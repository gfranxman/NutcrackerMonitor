#! /usr/bin/env python
import argparse

import socket
import json
from pprint import pprint

parser = argparse.ArgumentParser(description='Nutcracker monitor')
parser.add_argument("server_addr", help="the nutcracker server hostname",)

parser.add_argument('--port', action='store',dest='server_port', help='the nutcracker port', default=22222, type=int )

parser.add_argument("pools", metavar='poolname',  nargs='*', help="one or more pool names, empty to list them")

args = parser.parse_args()

pprint( args )


conn = socket.create_connection( (args.server_addr, args.server_port) )
buf = True
content = ''
while buf:
    buf = conn.recv( 1024*100 )
    content += buf
conn.close()

data = json.loads( content )

if not args.pools:
    # print the server's stats
    addr_str = "%s:%d" % ( args.server_addr, args.server_port )
    print addr_str
    print "=" * len( addr_str )
    for k in data.keys():
        if k[0] == k[0].lower():
            print "%10s : %s" % ( k, data[k] )

    # display the available pools
    print "\n\n"
    print "available pools (backends/connections/server_ejections)"
    print "======================================================="
    for k in sorted( data.keys() ):
        if k[0] == k[0].upper():
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
            print "%25s ( %d/%d/%d ) %s" % ( k, num_of_backends, client_connections, server_ejects, footnote )

else:
    for pool in args.pools:
        print "Pool:", pool
        pprint( data[pool] )

