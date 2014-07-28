import argparse
import shapefile
import numpy as np
import json

# Buildings attributes index.
BID = 0
ROAD = 3
LEFT = 4
RIGHT = 5

# Intersections attributes index.
ROADS = 0
ROADCLASSES = 1
IID = 3

# Attributes for final output.
NODETYPE = 2
CLASS = {'none': -1, 'service': 0, 'residential': 1, 'tertiary': 2}

def gen_blocks(buildings, intersections):
    bsf = shapefile.Reader(buildings)
    bshapes = bsf.shapes()
    brecords = bsf.records()
    isf = shapefile.Reader(intersections)
    ishapes = isf.shapes()
    irecords = isf.records()
    seen = set()
    intersection_points = {}
    road_data = {}
    roadclasses = {}
    for i, intersection in enumerate(ishapes):
        intersection_points[irecords[i][IID]] = intersection.points[0]
        roads = np.array(irecords[i][ROADS].split(','), dtype=int)
        road_data[irecords[i][IID]] = roads
        classes = np.array(irecords[i][ROADCLASSES].split(','))
        for j, road in enumerate(roads):
            roadclasses[road] = CLASS[classes[j]]
    iids = intersection_points.keys()
    buildings = {}
    for building in brecords:
        buildings[building[BID]] = building
    blocks = []
    bids = buildings.keys()
    for i, building in enumerate(bshapes):
        if (brecords[i][BID]) in seen:
            continue
        left = brecords[i][BID]
        right = brecords[i][BID]
        block = [brecords[i][BID]]
        while left in bids or right in bids:
            if left in bids:
                seen.add(left)
                left = buildings[left][LEFT]
                block = [left] + block
            if right in bids:
                seen.add(right)
                right = buildings[right][RIGHT]
                block = block + [right]
        blocks.append(block)
    counter = 0
    offset = 100000 # ID offset
    block_data = []
    block_edges = {}
    block_ids = {}
    for block in blocks:
        if block[0] in iids and block[-1] in iids:
            start = np.array(intersection_points[block[0]])
            end = np.array(intersection_points[block[-1]])
            road_class = -2
            # Get the road class.
            for a in road_data[block[0]]:
                for b in road_data[block[-1]]:
                    if a == b and a != -1:
                        road_class = roadclasses[a]
            if road_class == -2:
                continue
            diff = end - start
            # TODO: Sanitize the values here instead of using the != hack.
            if block[0] != block[-1]:
                theta = np.arctan(diff[0]/diff[1])
                block_data.append({'key':counter + offset,\
                        'attr': {'length': -1,\
                            'height': -1,\
                            'angle': theta,\
                            'roadClass': road_class,\
                            'degree': -1,\
                            'nodeType': NODETYPE},
                        'x': -1,\
                        'y': -1})
                edges = block[1:-1]
                if ((block[0], block[-1]) in block_ids):
                    key = block_ids[(block[0], block[-1])]
                    block_edges[str(key)] += list(set(edges) - set(block_edges[str(key)]))
                else:
                    block_edges[str(counter+offset)] = edges
                block_ids[(block[0], block[-1])] = counter + offset
                block_ids[(block[-1], block[0])] = counter + offset
                counter += 1
            else:
                continue
    output = json.dumps(block_data)
    output_edges = json.dumps(block_edges)
    fn = open("road.nodes.json", "w+")
    fe = open("road.edges.json", "w+")
    fn.write(output)
    fn.close()
    fe.write(output_edges)
    fe.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Infers road blocks from structure of building points.')
    parser.add_argument('buildings', help='shapefile that contains linked buildings')
    parser.add_argument('intersections', help='shapefile that contains intersections that were linked along with the buildings')
    args = parser.parse_args()
    buildings = args.buildings
    intersections = args.intersections
    gen_blocks(buildings, intersections)
    pass
