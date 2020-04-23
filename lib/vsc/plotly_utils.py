#!/usr/bin/env python

from configparser import ConfigParser
import datetime
import plotly.plotly as py
from plotly.graph_objs import Font

def create_annotations():
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    anno_text = 'time: {0}'.format(timestamp)
    annotations = [
        dict(
            text=anno_text,  # set annotation text
            showarrow=False, # remove arrow 
            xref='paper',  # use paper coords
            yref='paper',  #  for both coordinates
            x=0.95,  # position's x-coord
            y=1.10,  #   and y-coord
            font=Font(size=16),    # increase font size (default is 12)
            bgcolor='#FFFFFF',     # white background
            borderpad=4            # space bt. border and text (in px)
        )
    ]
    return annotations

def sign_in(file_name):
    '''read credentials from config file, and log in to Plot.ly'''
    conf_parser = ConfigParser()
    conf_parser.read(file_name)
    username = conf_parser.get('authentication', 'username')
    api_key = conf_parser.get('authentication', 'api_key')
    py.sign_in(username, api_key)
