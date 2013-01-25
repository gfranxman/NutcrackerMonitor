#! /usr/bin/env python

'''
    since nutcracker's status structure doesnt separate its backends from 
    its other configuration details we'll assume those with colons are backends
    maybe I should submit a patch that better organizes the stats structure
'''

import argparse # python2.7+
import socket
import json

class NutcrackerServer( object ):
    def __init__( self, server='127.0.0.1', port='22222' ):
        self.server = server
        self.port = port

        self.stats = {}
        self.active_pools = []
        self.inactive_pools = []
        self.broken_pools = []

        self._refresh_data()
        self._parse_data()


    def _refresh_data( self ):
        conn = socket.create_connection( (self.server, self.port) )
        buf = True
        content = ''
        while buf:
            buf = conn.recv( 1024 )
            content += buf
        conn.close()
    
        self.data = json.loads( content )


    def _parse_data( self ):
        active_pools = []
        inactive_pools = []
        broken_pools = []
        stats = {}

        for k in sorted( self.data.keys() ):
            try:
                v = self.data[k]
                # just to prove we are looking at a key for a backend server
                v['server_ejects'] 
              
                client_connections = v['client_connections']
                server_ejects = v['server_ejects']
                num_of_backends = 0

                for bk in v.keys():
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
            except (TypeError, KeyError), not_a_backend:
                stats[k] =  v

        self.stats = stats
        self.active_pools = sorted( active_pools ) 
        self.inactive_pools = sorted( inactive_pools ) 
        self.broken_pools = sorted( broken_pools ) 
        self.all_pools = sorted( self.active_pools + self.inactive_pools + self.broken_pools )



def display_server_status(nutcracker):
    addr_str = "%s:%d" % ( nutcracker.server, nutcracker.port )
    print addr_str
    print "=" * len( addr_str )
    for k,v in nutcracker.stats.items():
        print "%10s : %s" % ( k, v )
    print "\n"



def display_pool_list( title, keys, nutcracker ):
    report_title = title + ' (backends/connections/server_ejections)'
    print report_title
    print "=" * len( report_title )

    for pool_name in sorted( keys ):
        pool = nutcracker.data[pool_name]
        client_connections = pool['client_connections']
        server_ejects = pool['server_ejects']
        num_of_backends = 0
        footnote = ''

        for bk in pool.keys():
            if ":" in bk:
                num_of_backends += 1

       
        if num_of_backends == 0:
            footnote += '*'
        if server_ejects > 0:
            footnote += '!'

        print "%25s ( %d/%d/%d ) %s" % ( 
            pool_name, 
            num_of_backends, client_connections, server_ejects, 
            footnote,
        )

    print "\n\n"



def parse_args():
    parser = argparse.ArgumentParser(description='ballgazer -- a TwemProxy/Nutcracker monitor')
    parser.add_argument("server_addr", help="the nutcracker server hostname",)
    parser.add_argument('--port', default=22222, type=int , dest='server_port', 
        action='store', help='the nutcracker port', )
    parser.add_argument("pools", metavar='poolname',  
        nargs='*', help="one or more pool names, empty to list them")

    return parser.parse_args()



def main():
    args = parse_args()
    nutcracker = NutcrackerServer( args.server_addr, args.server_port )

    # display
    if not args.pools:
        # print the server's stats
        display_server_status( nutcracker )
    
        # display the available pools
        display_pool_list( "Broken", nutcracker.broken_pools, nutcracker ) 
        display_pool_list( "Active", nutcracker.active_pools, nutcracker ) 
        display_pool_list( "Unused", nutcracker.inactive_pools, nutcracker ) 

    else:
        for pool in args.pools:
            nutcracker.data[pool][u'pool_name'] = pool # inject the poolname so the result looks like valid json
            print( json.dumps( nutcracker.data[pool], indent=4)  )



if __name__ == '__main__':
    main()
