import argparse

parser = argparse.ArgumentParser(description='test panda parser')
parser.add_argument('-j', type=str, required=False, default='', help='j')
parser.add_argument('--sourceURL', type=str, required=False, default='', help='source url')
parser.add_argument('-r', type=str, required=False, default='', help='directory')
parser.add_argument('-l', required=False, action='store_true', default=False, help='l')
parser.add_argument('-o', type=str, required=False, default='', help='output')
parser.add_argument('-p', type=str, required=False, default='', help='parameter')

str = "-j \\ --sourceURL https://pandaserver-doma.cern.ch:25443 -r . -l  -o \'{'myout.txt': 'user.wguan.151988._000003.myout.txt'}\' -p \'python%20test_parsl_funcx.py%20%3E%20myout.txt\'"

arg_list = ["-j", "\\", "--sourceURL", "https://pandaserver-doma.cern.ch:25443", "-r", ".", "-l",  "-o", "\'{'myout.txt': 'user.wguan.151988._000003.myout.txt'}\'", "-p", "\'python%20test_parsl_funcx.py%20%3E%20myout.txt\'"]

args = parser.parse_args(arg_list)
print(args)

import urllib.request as urllib

p = urllib.unquote(args.p)
print(p)
