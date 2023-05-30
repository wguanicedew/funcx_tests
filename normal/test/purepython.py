import sys
print sys.argv
f = open('out.dat','w')
f.write('hello')
f.close()
sys.exit(0)
