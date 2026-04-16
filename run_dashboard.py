#!/usr/bin/env python3
"""Wrapper that sets cwd before launching Streamlit, needed for preview runner compatibility."""
import os
import sys

# Change to the project directory so Streamlit's Path.cwd() calls succeed
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Now invoke streamlit as if called from the command line
from streamlit.web import cli as stcli
sys.argv = ["streamlit", "run", os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.py"), "--server.port", "8501", "--server.headless", "true"]
sys.exit(stcli.main())
