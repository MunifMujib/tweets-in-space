#!/usr/bin/python3

import json
from shapely.geometry import Point, Polygon, MultiPolygon
from sqlitedict import SqliteDict

def load_gadm_poly(gadm_id, gadm):
    poly = MultiPolygon([Polygon(c[0]) for c in gadm[gadm_id]["geometry"]["coordinates"]])
    return poly

def find_supremum(place_id, places_dir, country_codes, gadm_index, gadm_db_path):
    output = {
        "place_id": place_id,
        "place_type": "",
        "supremum_gadm_id": "",
        "score": 0.0,
        "status": "unknown"
    }

    gadm = SqliteDict(gadm_db_path)

    try:
        place = json.load(open(places_dir + place_id + ".json", "r"))
        assert(place)
    except:
        output["status"] = "FaultyJSONError"
        return output

    if place["place_type"] == "poi":
        output["place_type"] = "poi"
        try:
            place_point = Point(place["centroid"])
        except:
            output["status"] = "NoCentroidError"
            return output

        a2_code = place["country_code"]
        country_code = ""
        if not a2_code:
            for a3_code in gadm_index:
                country_poly = load_gadm_poly(gadm_index[a3_code][0], gadm)
                if country_poly.contains(place_point):
                    country_code = a3_code
                    break
            if not country_code:
                output["status"] = "NoCountryError"
                return output
        else:
            try:
                country_code = country_codes[a2_code]
            except:
                output["status"] = "UnkownCountryError"
                return output

        gadm_ids = gadm_index[country_code]

        supremum_area = float("inf")
        for gadm_id in gadm_ids:
            try:
                poly = load_gadm_poly(gadm_id, gadm)

                if poly.contains(place_point):
                    if poly.area < supremum_area:
                        supremum_area = poly.area
                        output["supremum_gadm_id"] = gadm_id
                        output["status"] = "envelop"
            except:
                continue

    else:
        output["place_type"] = place["place_type"]
        try:
            place_bbox = Polygon(place["bounding_box"]["coordinates"][0])
        except:
            output["status"] = "FaultyBoundingBoxError"
            return output

        a2_code = place["country_code"]
        country_code = ""
        if not a2_code:
            country_intersect = 0.0
            for a3_code in gadm_index:
                country_poly = load_gadm_poly(gadm_index[a3_code][0], gadm)
                if place_bbox.intersection(country_poly).area > country_intersect:
                    country_code = a3_code
                    break
            if not country_code:
                output["status"] = "NoCountryError"
                return output
        else:
            try:
                country_code = country_codes[a2_code]
            except:
                output["status"] = "UnkownCountryError"
                return output

        gadm_ids = gadm_index[country_code]
        for gadm_id in gadm_ids:
            try:
                poly = load_gadm_poly(gadm_id, gadm)

                if place_bbox.intersects(poly):
                    intersect = place_bbox.intersection(poly).area
                    union = place_bbox.union(poly).area
                    jaccard = intersect / union

                    if jaccard > output["score"]:
                        output["status"] = "intersect"
                        output["supremum_gadm_id"] = gadm_id
                        output["score"] = jaccard
                        if poly.contains(place.bbox):
                            output["status"] = "envelop"

            except:
                continue

    return output


def run_construction(place_ids, places_dir = "../data/places/", country_codes_path = "../data/gadm/country-codes.json", gadm_index_path = "../data/gadm/gadm-index.json", gadm_db_path = "../data/gadm/gadm.db"):
    country_codes = json.load(open(country_codes_path, "r"))
    gadm_index = json.load(open(gadm_index_path, "r"))

    results = {}
    for place_id in place_ids:
        output = find_supremum(place_id, places_dir, country_codes, gadm_index, gadm_db_path)
        results[place_id] = output

    return results
