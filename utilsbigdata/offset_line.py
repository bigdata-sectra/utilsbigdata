# offseting lines a little so they dont overlap
import math

def offset_line(tuple_lat_lon, dn_input = 0, de_input = 0):
    
    #get lat lon
    lat = tuple_lat_lon[0]
    lon = tuple_lat_lon[1]
    
    #Earth’s radius, sphere
    R = 6378137

    #offsets in meters
    dn = dn_input
    de = de_input

    #Coordinate offsets in radians
    dLat = dn/R
    dLon = de/(R*math.cos(math.pi*lat/180))

    #OffsetPosition, decimal degrees
    latO = lat + dLat * 180/math.pi
    lonO = lon + dLon * 180/math.pi
    
    # return tuple
    return (latO, lonO)