#!/usr/bin/python

# This script listens to one tcp port for trigger signal from Crestron controller and send pwr control cmd to PCs
import SocketServer
import socket
import subprocess
import shlex


class UDPHandler(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        self.data = self.request[0].strip()
        print 'CMD for computer reset received: ' + self.data
        passwd = "'$upportQ123'"
        print self.data
        if "DDR_Win" in self.data:
            name = 'brownddr1'
        elif "DDR_Linux" in self.data:
            name = 'brownddr2'
        elif "DDR_HQ" in self.data:
            name = 'brownddr3'
        elif "QIN_Win" in self.data:
            name = 'brownqin1'
        elif "QIN_Linux" in self.data:
            name = 'brownqin2'
        elif "QIN_HQ" in self.data:
            name = 'brownqin3'
        else:
            return
        cmd_line = "wsman -h " + name + " -P 16992 -u admin -p " + passwd +\
                   " invoke http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_PowerManagementService" \
                   " -a RequestPowerStateChange -J /etc/power_cycle.xml"
        print cmd_line
        p = subprocess.Popen(shlex.split(cmd_line), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (xml_out, err) = p.communicate(timeout=5)
        cmd_line = "wsman -h " + name + " -P 16992 -u admin -p " + passwd +\
                   " invoke http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_PowerManagementService" \
                   " -a RequestPowerStateChange -J /etc/power_on.xml"
        p = subprocess.Popen(shlex.split(cmd_line), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (xml_out, err) = p.communicate(timeout=5)
if __name__ == "__main__":
    HOST, PORT = socket.gethostbyname(socket.gethostname()), 9999

    # Create the server, binding to localhost on port 9999
    server = SocketServer.UDPServer((HOST, PORT), UDPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
