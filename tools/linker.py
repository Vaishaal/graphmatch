import argparse
import shapefile
import numpy as np
from math import pi
from scipy.spatial import *
import utm
from collections import defaultdict
import json
import pickle

PIPE_WIDTH = 20 # Max width of the pipe in meters.
ANGLE_TOLERANCE = pi/8 # Angle of tolerance that we can call straight for determining T intersections.
DEGREE = 1 # Field of the intersections record that contains degree.
OVERLAP_THRESHOLD = 5 # Number of meters the centroids of two buildings have to be apart to count as non-overlapping.

# Matcher node types.
BUILDING = 0
INTERSECTION = 1
ROAD = 2

# Intersection shapefile record fields.
INTERSECTING_ROADS = 0

# Road shapefile record fields.
ROAD_ID = 0
CLASS = 1
CLASS_MAP = {'service': 0,
        'residential': 1,
        'unclassified': 2,
        'tertiary': 3,
        'secondary': 4,
        'primary': 5,
        'trunk': 6,
        'motorway': 7,
        'track': 8,
        'primary_link': 9}

def link(building_sf_path, intersection_sf_path, road_sf_path, visualization_sf_path):
    # Read in the shapefiles.
    build_sf = shapefile.Reader(building_sf_path) # Polygons.
    int_sf = shapefile.Reader(intersection_sf_path) # Points.
    road_sf = shapefile.Reader(road_sf_path) # Series of line segments.
    irecords = int_sf.records()
    rrecords = road_sf.records()

    # Convert everything to UTM.
    buildings_utm = map(lambda building: [lonlat_to_utm(corner) for corner in building.points], build_sf.shapes())
    intersections_utm = map(lambda intersection: lonlat_to_utm(intersection.points[0]), int_sf.shapes())
    roads_utm = map(lambda road: [lonlat_to_utm(corner) for corner in road.points], road_sf.shapes())

    # Create a set of registered intersections.
    intersection_set = set()
    # Find the intersections that have T intersections.
    tees = set()
    intersection_id = {} # Intersection IDs start after building IDs.
    intersection_base = len(buildings_utm)
    for i, intersection in enumerate(intersections_utm):
        intersection_set.add(tuple(intersection))
        intersection_id[tuple(intersection)] = intersection_base + i
        if irecords[i][DEGREE] == 3:
            tees.add(tuple(intersection))

    # Generate a KDTree of buildings to facilitate linking.
    building_centroids = np.zeros([2, len(build_sf.shapes())])
    centroid_to_building = {}
    for i in range(len(build_sf.shapes())):
        centroid = compute_centroid(buildings_utm[i])
        building_centroids[:,i] = centroid
        centroid_to_building[centroid] = buildings_utm[i]
    centroid_tree = KDTree(building_centroids)

    # Go through road DB to find the bottom tip of each T intersection (one on a different line from the other two.
    tee_tips = {}
    for road in roads_utm:
        if tuple(road[0]) in tees:
            tee_tips[tuple(road[0])] = road[1]
        if tuple(road[-1]) in tees:
            tee_tips[tuple(road[-1])] = road[-2]

    # Breaks roads down into segments. Fills in dictionary for T intersections to the adjacent road nodes.
    # Makes map from segment to OSM id.
    road_to_segments = defaultdict(list)
    segment_to_osm = {}
    for road_index, road in enumerate(roads_utm):
        for i in range(len(road) - 1):
            road_to_segments[road_index].append((tuple(road[i]), tuple(road[i+1])))
            road_to_segments[road_index].append((tuple(road[i+1]), tuple(road[i])))
            segment_to_osm[(tuple(road[i]), tuple(road[i+1]))] = rrecords[road_index][ROAD_ID]
            segment_to_osm[(tuple(road[i+1]), tuple(road[i]))] = rrecords[road_index][ROAD_ID]

    # Creates a map from OSM road id to a list of OSM roads that intersect it.
    road_to_intersecting_roads = defaultdict(set)
    for intersection_attrs in irecords:
        road_str = intersection_attrs[INTERSECTING_ROADS]
        intersecting_roads = road_str.split(',')
        for r1 in intersecting_roads:
            r1 = int(r1)
            for r2 in intersecting_roads:
                r2 = int(r2)
                if r1 != r2 and r1 != -1 and r2 != -1:
                    road_to_intersecting_roads[r1].add(r2)

    # Maps a road segment and a whole road to a list of nearby buildings.
    segment_to_buildings = defaultdict(list)
    road_to_buildings = defaultdict(list)
    rot90 = np.matrix([[0, -1], [1, 0]])
    for i, building in enumerate(building_centroids.T):
        if i % 100 == 0:
            print i
            """
        if i < 638:
            continue
        elif i > 665:
            break
            """
        candidate_segments = []
        for road_index in road_to_segments:
            segments = road_to_segments[road_index]
            for segment in segments:
                segment_vector = np.matrix(segment[1]) - np.matrix(segment[0])
                segment_length = np.linalg.norm(segment_vector)
                segment_vector = segment_vector/segment_length
                perp_vector = segment_vector*rot90
                # Solve for the intersection of the segment line and the perpendicular passing through the buildling.
                solution = np.linalg.pinv(np.concatenate([segment_vector, -perp_vector]).T)*((np.matrix(building) - np.matrix(segment[0])).T)
                # solution[1] is the distance from building to segment
                if solution[0] > 0 and solution[0] < segment_length and solution[1] > 0 and solution[1] < PIPE_WIDTH:
                    candidate_segments.append((segment, float(solution[1]), float(solution[0])))
        if len(candidate_segments) == 0:
            continue
        candidate_segments.sort(key=lambda x: x[1])
        accepted_segments = [candidate_segments[0][0]]
        last_accepted = candidate_segments[0][0]
        road_to_buildings[segment_to_osm[last_accepted]].append(i)
        # Store the building index and distance ALONG segment as well as distance FROM segment.
        segment_to_buildings[last_accepted].append((i, candidate_segments[0][2], candidate_segments[0][1])) 
        # Add all segments that meet the crtieria.
        candidate_segments = candidate_segments[1:]
        last_len = len(candidate_segments)
        while True:
            for seg in candidate_segments:
                last_osm = segment_to_osm[last_accepted]
                next_osm = segment_to_osm[seg[0]]
                if next_osm in road_to_intersecting_roads[last_osm]:
                    last_accepted = seg[0]
                    road_to_buildings[segment_to_osm[last_accepted]].append(i)
                    accepted_segments.append(last_accepted)
                    segment_to_buildings[last_accepted].append((i, seg[2], seg[1]))
                    candidate_segments.remove(seg)
                    break
            if len(candidate_segments) == last_len:
                break
            else:
                last_len = len(candidate_segments)

    # Create edges.
    edges = defaultdict(list)
    left_merge = defaultdict(list) # If one of the ends of a segment isn't an intersection, it needs to get stithced to another segment.
    right_merge = defaultdict(list) # We don't need any additional data structures here because ...
    for segment in segment_to_buildings:
        buildings = segment_to_buildings[segment]
        """
        if segment[0] in intersection_id:
            if intersection_id[segment[0]] == 3038:
                print "left: %d" % intersection_id[segment[0]]
                print buildings
        if segment[1] in intersection_id:
            if intersection_id[segment[1]] == 3034:
                print "right: %d" % intersection_id[segment[1]]
                print buildings
        """
        buildings.sort(key=lambda x: x[1])
        prune_interior_buildings(buildings, segment, buildings_utm)
        # First handle all the buildlings in the middle of the segment.
        for i in range(1, len(buildings)-1):
            c = buildings[i] # c for current
            p = buildings[i-1] # p for previous
            n = buildings[i+1] # n for next
            edges[str(buildings[i][0])].append(buildings[i-1][0])
            edges[str(buildings[i][0])].append(buildings[i+1][0])

        # Process the leftmost building of the segment.
        if segment[0] in tees:
            other_tip = tee_tips[segment[0]]
            seg_vector = np.array(segment[-1]) - np.array(segment[0])
            branch_vector = np.array(other_tip) - np.array(segment[0])
            determination = np.cross(branch_vector, seg_vector)
            if determination > 0:
                left_merge[segment[0]].append((buildings[0][0], segment)) # Only need building ID
                if len(buildings) > 1:
                    edges[str(buildings[0][0])].append(buildings[1][0])
            else:
                edges[str(buildings[0][0])].append(intersection_id[segment[0]])
                if len(buildings) > 1:
                    edges[str(buildings[0][0])].append(buildings[1][0])
        elif segment[0] in intersection_set:
            edges[str(buildings[0][0])].append(intersection_id[segment[0]])
            if len(buildings) > 1:
                edges[str(buildings[0][0])].append(buildings[1][0])
        else:
            left_merge[segment[0]].append((buildings[0][0], segment))

        # Process the rightmost building of the segment.
        if segment[-1] in tees:
            other_tip = tee_tips[segment[-1]]
            seg_vector = np.array(segment[0]) - np.array(segment[-1])
            branch_vector = np.array(other_tip) - np.array(segment[-1])
            determination = np.cross(seg_vector, branch_vector)
            if determination > 0: # Tells that we are on the top side of the T for this T intersection.
                right_merge[segment[-1]].append((buildings[-1][0], segment))
                if len(buildings) > 1:
                    edges[str(buildings[-1][0])].append(buildings[-2][0])
            else:
                edges[str(buildings[-1][0])].append(intersection_id[segment[-1]])
                if len(buildings) > 1:
                    edges[str(buildings[-1][0])].append(buildings[-2][0])
        elif segment[-1] in intersection_set:
            edges[str(buildings[-1][0])].append(intersection_id[segment[-1]])
            if len(buildings) > 1:
                edges[str(buildings[-1][0])].append(buildings[-2][0])
        else:
            right_merge[segment[-1]].append((buildings[-1][0], segment)) # Segments that need a merge on the left.

        # Even for the two buildings on the far ends, we always add the edges towards the middle of the segment.
        if len(buildings) > 1:
            edges[str(buildings[0][0])].append(buildings[1][0])
            edges[str(buildings[-1][0])].append(buildings[-2][0])

    pkl = open('debug.pkl', 'w')
    pickle.dump([dict(segment_to_buildings), dict(left_merge), dict(right_merge), dict(edges)], pkl)

    for point in left_merge:
        if point in right_merge:
            for right, left_segment in left_merge[point]:
                for left, right_segment in right_merge[point]:
                    if set(left_segment) != set(right_segment): # To prune out cul-de-sacs
                        angle = get_angle(np.array(left_segment[1]) - np.array(left_segment[0]),\
                                np.array(right_segment[1]) - np.array(right_segment[0]))
                        if angle < ANGLE_TOLERANCE:
                            edges[str(right)].append(left)
                            edges[str(left)].append(right)

    # Generate visualization shapefiles.
    if visualization_sf_path != '':
        w = shapefile.Writer(shapefile.POLYLINE)
        w.field('ID', 'N', '6')
        counter = 0
        for a in edges:
            if int(a) >= intersection_base:
                point_a = intersections_utm[int(a)]
            elif int(a) < intersection_base:
                point_a = building_centroids[:,int(a)]
            for b in edges[a]:
                if b >= intersection_base:
                    point_b = intersections_utm[b - intersection_base]
                elif b < intersection_base:
                    point_b = building_centroids[:,b]
                w.poly(shapeType=shapefile.POLYLINE, parts=[[utm_to_lonlat(point_a), utm_to_lonlat(point_b)]])
                w.record(counter)
                counter += 1
        w.save(visualization_sf_path + '.lines.shp')
        wp = shapefile.Writer(shapefile.POINT)
        wp.field('ID', 'N', '4')
        for i, centroid in enumerate(building_centroids.T):
            centroid = utm_to_lonlat(centroid)
            wp.point(centroid[0], centroid[1])
            wp.record(i)
        for intersection in intersection_id:
            int_id = intersection_id[intersection]
            intersection = utm_to_lonlat(intersection)
            wp.point(intersection[0], intersection[1])
            wp.record(int_id)
        wp.save(visualization_sf_path + '.points.shp')

    # Add in road edges AFTER the visualization.
    for road_id in road_to_buildings:
        for building in road_to_buildings[road_id]:
            edges[str(road_id)].append(building)

    # Visualize road nodes.
    if visualization_sf_path != '':
        wr = shapefile.Writer(shapefile.POLYLINE)
        wr.field('ID', 'N', '10')
        wrp = shapefile.Writer(shapefile.POINT)
        wrp.field('ID', 'N', '10')
        for index, road in enumerate(road_sf.shapes()):
            x = 0
            y = 0
            for point in road.points:
                x += point[0]
                y += point[1]
            x = x/len(road.points)
            y = y/len(road.points)
            wrp.point(x,y)
            wrp.record(rrecords[index][ROAD_ID])
            for building in edges[str(rrecords[index][ROAD_ID])]:
                tmp = (building_centroids.T)[building]
                if len(tmp) > 0:
                    wr.poly(shapeType=shapefile.POLYLINE, parts=[[utm_to_lonlat(tmp), [x,y]]])
                    wr.record(0)
        wr.save(visualization_sf_path + '.road_edges.shp')
        wrp.save(visualization_sf_path + '.road_nodes.shp')


    # Sanitize edges: remove duplicates.
    for key in edges:
        edges[key] = list(set(edges[key]))

    # Output json files.
    output_edges = json.dumps(edges)
    fe = open('db.edges.json', 'w+')
    fe.write(output_edges)
    fe.close()
    node_data = []
    for i, centroid in enumerate(building_centroids.T):
        node_data.append({'key': i,\
                'attr': {'length': -1,\
                    'height': -1,\
                    'angle': 0,\
                    'roadClass': -1,\
                    'degree': -1,\
                    'nodeType': BUILDING},\
                'x': centroid[0],\
                'y': centroid[1]})
    for i, intersection in enumerate(intersections_utm):
        node_data.append({'key': intersection_base + i,\
                'attr': {'length': -1,\
                    'height': -1,\
                    'angle': 0,\
                    'roadClass': -1,\
                    'degree': irecords[i][DEGREE],\
                    'nodeType': INTERSECTION},\
                'x': intersection[0],\
                'y': intersection[1]})
    for road_attrs in rrecords:
        node_data.append({'key': road_attrs[ROAD_ID],\
                'attr': {'length': -1,\
                    'height': -1,\
                    'angle': 0,\
                    'roadClass': CLASS_MAP[road_attrs[CLASS]],\
                    'degree': -1,\
                    'nodeType': ROAD},\
                'x': -1,\
                'y': -1})
    output_nodes = json.dumps(node_data)
    fn = open('db.nodes.json', 'w+')
    fn.write(output_nodes)
    fn.close()

####################
# Helper Functions #
####################
def lonlat_to_utm(point):
    output = utm.conversion.from_latlon(point[1], point[0]) # Swap indices for lonlat.
    return [output[0], output[1]]

def utm_to_lonlat(point):
    output = utm.to_latlon(point[0], point[1], 37, 'S')
    return [output[1], output[0]]

def compute_centroid(polygon):
    # Implements the formula on the wikipedia page.
    A = 0 # A for Area
    Cx = 0 # Centroid x coordinate
    Cy = 0 # y coordinate
    for i in range(len(polygon) - 1):
        tmp = polygon[i][0]*polygon[i+1][1] - polygon[i+1][0]*polygon[i][1]
        A += tmp
        Cx += (polygon[i][0] + polygon[i+1][0])*tmp
        Cy += (polygon[i][1] + polygon[i+1][1])*tmp
    A = A/2
    Cx = Cx/(6*A)
    Cy = Cy/(6*A)
    return (Cx, Cy)

def get_angle(u, v):
    return abs(float(np.arccos((u/np.linalg.norm(u)).dot(v/np.linalg.norm(v)))))

def prune_interior_buildings(buildings, segment, building_db):
    # If a building is somehow caught, but is behind another building we get rid of it.
    # To do this we see if two buildings that are adjacent to each other have footprints
    # that overlap in the projection down to the segment.
    # The building whose centroid is closer to the road wins and the other the removed.
    to_remove = set()
    for i in range(len(buildings) - 1):
        poly_a = building_db[buildings[i][0]]
        poly_b = building_db[buildings[i+1][0]]
        # We find the rightmost point in poly_a and the leftmost in poly_b, along the direction of the segment.
        base = np.array(segment[0])
        segment_vector = np.array(segment[1]) - base
        rightmost = max(map(lambda x: (np.array(x) - base).dot(segment_vector), poly_a))
        leftmost = min(map(lambda x: (np.array(x) - base).dot(segment_vector), poly_b))
        if leftmost < rightmost and abs(buildings[i][2] - buildings[i+1][2]) > OVERLAP_THRESHOLD:
            if buildings[i][2] < buildings[i+1][2]:
                to_remove.add(buildings[i+1])
            elif buildings[i][2] > buildings[i+1][2]:
                to_remove.add(buildings[i])
    for item in to_remove:
        buildings.remove(item)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Links buildings and intersections for the database.')
    parser.add_argument('buildings', type=str, help='Shapefile containing building footprints.')
    parser.add_argument('intersections', type=str, help='Shapefile containing intersection points.')
    parser.add_argument('roads', type=str, help='Shapefile containing roads.')
    parser.add_argument('--visualize', help='Write the linked output as a shapefile for visualization to the specified filename.', default='')
    args = parser.parse_args()
    link(args.buildings, args.intersections, args.roads, args.visualize)
