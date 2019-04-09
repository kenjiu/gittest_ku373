
def process(pid, records):
    
    import csv
    import pyproj
    import shapely.geometry as geom

    import fiona
    import fiona.crs
    import shapely
    import rtree

    import pandas as pd
    import geopandas as gpd
    import json






    neighborhoods = gpd.read_file("neighborhoods.geojson").to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()


    for idx,geometry in enumerate(neighborhoods.geometry):
        index.insert(idx, geometry.bounds)
    #return (index, zones)

    for idx1,geometry in enumerate(neighborhoods.geometry):
        index.insert(idx1, geometry.bounds)

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    counts = {}
    


    reader = csv.reader(records)
    if pid==0:
        next(records)
        #next(reader) 
    for row in reader:
        match = None
        boro = None
        
        try:
            p = geom.Point(proj(float(row[5]), float(row[6]))) ##Just making a POINT data.'pickup_latitude','pickup_longitude
            p1 = geom.Point(proj(float(row[9]), float(row[10]))) # Dropoff
        
            for idx1 in index.intersection((p1.x, p1.y, p1.x, p1.y)):
                if neighborhoods.geometry[idx1].contains(p1):
                    boro = neighborhoods.borough[idx1]
                    break
        
            for idx in index.intersection((p.x, p.y, p.x, p.y)): 
                if neighborhoods.geometry[idx].contains(p):
                    match = neighborhoods.neighborhood[idx]
                    break
        except Exception:
            pass
        
        if match and boro:
            combname = tuple((boro,match))
            counts[combname] = counts.get(combname, 0) + 1               

    return counts.items()








if __name__ =='__main__':
    from pyspark import SparkContext
    import sys   
    sc = SparkContext()
    test2 = sys.argv[1]
    #neighborhoodsgeojson = sys.argv[1]    
    
    rdd = sc.textFile(test2)
    rdd = rdd.mapPartitionsWithIndex(test).reduceByKey(lambda x,y:x+y).filter(lambda x: None not in x[0]).sortBy(lambda x:(x[0][0],x[1]),ascending=False).groupBy(lambda x: x[0][0]).flatMap(lambda x:x[1]).collect()

    print(rdd.mapPartitionsWithIndex(process).collect()) #.reduce(lambda x,y:x+y))

