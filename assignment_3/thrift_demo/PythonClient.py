#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys, glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('lib-py')[0])

from demo import MathService
from demo.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


try:

  # Make socket
  transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))
  # Wrap socket in framed transport
  transport = TTransport.TFramedTransport(transport)
  # Use binary protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)

  # Create a client
  client = MathService.Client(protocol)

  # Connect the socket
  transport.open()

  inarg = int(sys.argv[3])
  try:
    sqrt = client.sqrt(inarg)
    print 'sqrt(%d)=%f' % (inarg, sqrt)
  except IllegalArgument, io:
    print 'Exception: %r' % io

  # Close the socket
  transport.close()


except Thrift.TException, tx:
  print '%s' % (tx.message)
