import argparse
import shapefile
import numpy as np
from math import pi
from scipy.spatial import *
import utm
from collections import defaultdict

PIPE_WIDTH = 20 # Max width of the pipe in meters.
ANGLE_TOLERANCE = 3,14/8 # Angle of tolerance that we can call straight for determining T intersections.
DEGREE = 1 # Field of the intersections record that contains degree.

def link(building_sf_path, intersection_sf_path, road_sf_path, visualization_sf_path):
    # Read in the shapefiles.
    build_sf = shapefile.Reader(building_sf_path) # Polygons.
    int_sf = shapefile.Reader(intersection_sf_path) # Points.
    road_sf = shapefile.Reader(road_sf_path) # Series of line segments.
    irecords = int_sf.records()

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
    road_to_segments = defaultdict(lambda: [])
    for road_id, road in enumerate(roads_utm):
        for i in range(len(road) - 1):
            road_to_segments[road_id].append((tuple(road[i]), tuple(road[i+1])))
            road_to_segments[road_id].append((tuple(road[i+1]), tuple(road[i])))

    # Maps a road segment to a list of nearby buildings.
    segment_to_buildings = defaultdict(lambda: [])
    building_to_segments = defaultdict(lambda: [])
    rot90 = np.matrix([[0, -1], [1, 0]])
    for i, building in enumerate(building_centroids.T):
        if i % 100 == 0:
            print i
        if i > 200:
            break
        candidate_segments = []
        for road_id in road_to_segments:
            segments = road_to_segments[road_id]
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
        # Store the building index and distance ALONG segment as well as distance FROM segment.
        segment_to_buildings[last_accepted].append((i, candidate_segments[0][2], candidate_segments[0][1])) 
        # Add all segments that meet the crtieria.
        candidate_segments = candidate_segments[1:]
        last_len = len(candidate_segments)
        while True:
            for seg in candidate_segments:
                if shares_intersection(last_accepted, seg[0], intersection_set):
                    last_accepted = seg[0]
                    accepted_segments.append(last_accepted)
                    segment_to_buildings[last_accepted].append((i, seg[2], seg[1]))
                    candidate_segments.remove(seg)
                    break
            if len(candidate_segments) == last_len:
                break
            else:
                last_len = len(candidate_segments)

    # Create edges.
    edges = defaultdict(lambda: [])
    left_merge = {} # If one of the ends of a segment isn't an intersection, it needs to get stithced to another segment.
    right_merge = {} # We don't need any additional data structures here because ...
    for segment in segment_to_buildings:
        buildings = segment_to_buildings[segment]
        buildings.sort(key=lambda x: x[1])
        prune_interior_buildings(buildings, segment, buildings_utm)
        for i in range(1, len(buildings)-1):
            c = buildings[i] # c for current
            p = buildings[i-1] # p for previous
            n = buildings[i+1] # n for next
            edges[str(buildings[i][0])].append(buildings[i-1][0])
            edges[str(buildings[i][0])].append(buildings[i+1][0])
        if segment[0] in tees:
            other_tip = tee_tips[segment[0]]
            seg_vector = np.array(segment[-1]) - np.array(segment[0])
            branch_vector = np.array(other_tip) - np.array(segment[0])
            determination = np.cross(branch_vector, seg_vector)
            if determination > 0:
                left_merge[segment[0]] = (buildings[0], segment[-1])
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
            left_merge[segment[0]] = (buildings[0], segment[-1]) # Segments that need a merge on the left.

        if segment[-1] in tees:
            other_tip = tee_tips[segment[-1]]
            seg_vector = np.array(segment[0]) - np.array(segment[-1])
            branch_vector = np.array(other_tip) - np.array(segment[-1])
            determination = np.cross(seg_vector, branch_vector)
            if determination > 0:
                right_merge[segment[-1]] = (buildings[-1], segment[-1])
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
            right_merge[segment[-1]] = (buildings[-1], segment[0]) # Segments that need a merge on the left.

    for point in left_merge:
        if point in right_merge:
            right, other_r = left_merge[point]
            left, other_l = right_merge[point]
            if other_r != other_l: # To prune out cul-de-sacs
                edges[str(right[0])].append(left[0])
                edges[str(left[0])].append(right[0])

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
                w.poly(shapeType=shapefile.POLYLINE, parts=[[list(point_a), list(point_b)]])
                w.record(counter)
                counter += 1
        w.save(visualization_sf_path + '.lines.shp')
        wp = shapefile.Writer(shapefile.POINT)
        wp.field('ID', 'N', '4')
        for i, centroid in enumerate(building_centroids.T):
            wp.point(centroid[0], centroid[1])
            wp.record(i)
        for intersection in intersection_id:
            wp.point(intersection[0], intersection[1])
            wp.record(intersection_id[intersection])
        wp.save(visualization_sf_path + '.points.shp')


####################
# Helper Functions #
####################
def lonlat_to_utm(point):
    output = utm.conversion.from_latlon(point[1], point[0]) # Swap indices for lonlat.
    return [output[0], output[1]]

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

def shares_intersection(seg1, seg2, intersections):
    if tuple(seg1[0]) in intersections or tuple(seg2[0]) in intersections or tuple(seg1[1]) in intersections or tuple(seg2[1]) in intersections:
        if seg1[0] == seg2[0] or\
            seg1[1] == seg2[0] or\
            seg1[0] == seg2[1] or\
            seg1[1] == seg2[1]:
            return True
    return False

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
        if leftmost < rightmost:
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
