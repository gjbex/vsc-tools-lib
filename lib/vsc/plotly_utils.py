#!/usr/bin/env python

import datetime
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

