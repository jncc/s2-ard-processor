# Hack to get mpi4py to build with conda https://bitbucket.org/mpi4py/mpi4py/issues/143/build-failure-with-python-installed-from
# Add this file to the mpi4py conf/ directory

# sysconfigdata-conda-user.py
import os, sysconfig
from distutils.util import split_quoted

_key = '_PYTHON_SYSCONFIGDATA_NAME'
_val = os.environ.pop(_key, None)
_name = sysconfig._get_sysconfigdata_name(True)

_data = __import__(_name, globals(), locals(), ['build_time_vars'], 0)
build_time_vars = _data.build_time_vars

def _fix_options(opt):
    if not isinstance(opt, str):
        return opt
    try:
        opt = split_quoted(opt)
    except:
        return opt
    try:
        i = opt.index('-B')
        del opt[i:i+2]
    except ValueError:
        pass
    try:
        i = opt.index('-Wl,--sysroot=/')
        del opt[i]
    except ValueError:
        pass
    return " ".join(opt)

for _key in build_time_vars:
    build_time_vars[_key] = _fix_options(build_time_vars[_key])