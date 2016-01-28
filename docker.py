import os,sys
import codecs
import subprocess
from subprocess import Popen,PIPE
import argparse
import json


p = Popen(['make'], shell=True, stdout=PIPE, stderr=PIPE)
stdout, stderr = p.communicate()

#print stdout
print stderr
print p.returncode

