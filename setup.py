#!/usr/bin/env python3

from setuptools import setup, Extension

ext = Extension(
    'atf_engine._convert',
    [
        'atf_engine/convert2j.cpp',
    ],
    extra_compile_args=['-Wall','-Werror'],
    define_macros=[('Py_LIMITED_API','0x030B0000')], # limited >= 3.11
    py_limited_api=True,
)

setup(ext_modules=[ext])
