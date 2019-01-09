#!/usr/bin/env python
# coding=utf-8
#
# Create a Google Earth KML visualization of the MGRS quad output of an m8b v1 query
#
# Author: arkasha
# Created 2019.01.08
# Copyright (c) 2018, Hugh Kennedy, Robert Hagemann, Andrew Carra
# All rights reserved.

from fastkml import kml, LineStyle, PolyStyle
from fastkml.geometry import Geometry
from shapely.geometry import Point, LineString, Polygon
from geopy.distance import vincenty
from colour import Color
import sys
import mgrs

class m8b_query_viz:
    def __init__(self):
        self.lines=None
        self.current_line=0
        self.mgrs = mgrs.MGRS()
        self.output_kml = kml.KML()
        self.name_space = '{http://www.opengis.net/kml/2.2}'
        self.cell_document=None
        self.gradient_values=None # list of colors
        self.threshold=0

    def main(self):
        with open(sys.argv[1], 'r') if len(sys.argv) > 1 else sys.stdin as f:
            # read the whole contents and put it in memory
            lines = f.readlines()
            f.close()

        for line in lines:
            if self.current_line > 2:
                # TODO: establish YUV axis if undef.
                components = line.strip().split()
                if self.gradient_values is None:
                    self.gradient_values = self.setup_gradient_and_t(int(components[1]))
                    # styles.append(kml.Style(self.name_space, styleid, this_style)

                if int(components[1]) > self.threshold:

                    style = self.gradient_values[int(components[1])-1] 

                    # southwest corner
                    sw_lat_lon = self.mgrs.toLatLon(str.encode(components[0]))
                    ne_lat_lon = self.offset_corner(sw_lat_lon)

                    pm_ident = 'mgrs-'
                    pm_ident += str(self.current_line)
                    style_identifier = "#style-"
                    style_identifier += components[1]
                    # print (f'{components[0]}: lat: {lat_lon[0]:.5f} lon: {lat_lon[1]:.5f} value: {components[1]} {northing} {easting}')
                    p = kml.Placemark(self.name_space, pm_ident, components[0], ''.join(components), None, style_identifier)
                    p.geometry = Geometry(self.name_space, '', Polygon([(sw_lat_lon[1], sw_lat_lon[0], 170), (ne_lat_lon[1], sw_lat_lon[0], 170), (ne_lat_lon[1], ne_lat_lon[0], 170), (sw_lat_lon[1], ne_lat_lon[0], 170)]), True, True, 'relativeToGround') 
                    self.cell_document.append(p)
                # process line
            self.current_line+=1

        self.output_kml.append(self.cell_document)
        print (self.output_kml.to_string(prettyprint=True))

    # hack to get the NE corner for the MGRS box (MGRS coords are the SW corner)
    def offset_corner(self, lat_lon):
        # testing hack - works for most of the canonical MGRS space
        # lat = lat_lon[0] + 0.009
        #lon = lat_lon[1] + 0.009

        calculator = vincenty(meters=999) # leaving a 1m gap
        lat = calculator.destination((lat_lon[0], lat_lon[1]), 0).latitude
        lon = calculator.destination((lat_lon[0], lat_lon[1]), 90).longitude

        return (lat, lon)

    # A generate styles for transition from green (highest) to red according to the steps in output
    def setup_gradient_and_t(self, steps):
        red = Color("blue")
        colors = list(red.range_to(Color("green"),steps))
        styles = []
        for idx, value in enumerate(colors):
            # DEBUG: print (f'VALUE: {value.hex_l}')
            style_id = "style-"
            idx_inc = idx+1
            style_id += str(idx_inc)
            lineStyle = LineStyle() #self.name_space,f'line-{idx_inc}',Color("white").hex_l)
            polyStyle = PolyStyle(self.name_space,f'poly-{idx_inc}',value.hex_l.replace('#', '#88'))
            this_style = kml.Style(self.name_space,style_id, (polyStyle, lineStyle))
            styles.append(this_style)

        self.cell_document = kml.Document(self.name_space, 'mgrs-doc', 'Cells', 'MGRS cells matching query', styles)
        # try and limit returns
        # self.threshold = steps - 1 if steps > 1 else 1
        self.threshold = 0
        return styles;

if __name__ == "__main__":
        viz=m8b_query_viz()
        viz.main()	
