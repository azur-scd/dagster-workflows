import glob
import os
import subprocess
import lxml

subprocess.run(['/bin/bash','docelec_signalement/run_saxon.sh','docelec_signalement/ftf/ftf4primo.xsl','docelec_signalement/ftf/02_intermediate/ftf_temp.xml','docelec_signalement/ftf/02_intermediate/ftf.xml'])