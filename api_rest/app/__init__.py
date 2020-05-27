from flask import Flask
from flask import jsonify, request, make_response
from datetime import datetime
from multiprocessing import Process, Manager
from app import temporal
from app import spatial
from app import model


app = Flask(__name__)
app.config['DEBUG'] = True


@app.route('/api/v1/ogdse/search/<query_level>')
def get_resources_or_dataset_ids_(query_level):
    interval_start = request.args.get("interval_start")
    interval_end = request.args.get("interval_end")
    gid_place = request.args.get('gid_place')
    name_place = request.args.get('name_place')
    # ----------------------------------------------------------------------------------------------------------------#
    if name_place and not gid_place:
        places = spatial.find_places(name_place)
        if len(places) > 1:
            return make_response(jsonify(places, 300))
        elif len(places) == 0:
            return make_response(jsonify(places, 200))
        else:
            gid_place = places[0]['gid']
    # ----------------------------------------------------------------------------------------------------------------#
    if interval_start and interval_end:
        try:
            interval_start = datetime.strptime(interval_start, "%d/%m/%Y").date()
            interval_end = datetime.strptime(interval_end, "%d/%m/%Y").date()
        except ValueError:
            return make_response(jsonify([], 400))

        if interval_start > interval_end:
            return make_response(jsonify([], 400))
    elif len([interval for interval in [interval_start, interval_end] if interval]) == 1:
        return make_response(jsonify([], 400))
    # ----------------------------------------------------------------------------------------------------------------#
    with Manager() as manager:
        if gid_place:
            spatial_query_return = manager.dict()
            p_spatial = Process(target=spatial_query, args=[gid_place, query_level, spatial_query_return])
            p_spatial.start()
            p_spatial.join()

        if interval_start and interval_end:
            temporal_query_return = manager.dict()
            p_temporal = Process(target=temporal_query_result,
                                 args=[interval_start, interval_end, query_level, temporal_query_return])
            p_temporal.start()
            p_temporal.join()

        results_queries = []
        if 'spatial_query_return' in locals():
            results_queries.append(spatial_query_return)
        if 'temporal_query_return' in locals():
            results_queries.append(temporal_query_return)
    # ----------------------------------------------------------------------------------------------------------------#
        results_queries.sort(key=lambda elem: len(elem))
        resources_or_dataset_dict = {}
        for result in results_queries[1:]:
            for resource_or_dataset_id in results_queries[0]:
                gt_dict_freq = result.get(resource_or_dataset_id)
                if gt_dict_freq:
                    resources_or_dataset_dict[resource_or_dataset_id] = \
                        (gt_dict_freq + results_queries[0][resource_or_dataset_id])/2
            results_queries[0] = resources_or_dataset_dict
        response = [{"id": key, "freq": results_queries[0][key]} for key in results_queries[0].keys()]
        response.sort(key=lambda elem: elem["freq"], reverse=True)
        response = make_response(jsonify(response, 200))
        return response


def spatial_query(gid_place, query_level, spatial_query_return):
    spatial_query_return.update(spatial.get_resources(gid_place)
                                if query_level == 'resource' else spatial.get_dataset(gid_place))


def temporal_query_result(interval_start, interval_end, query_level, temporal_query_return):
    temporal_query_return.update(temporal.get_resource(interval_start, interval_end)
                                 if query_level == 'resource' else temporal.get_dataset(interval_start, interval_end))


@app.route('/api/v1/ogdse/search/<query_level>', methods=['POST'])
def get_resources_or_dataset(query_level):
    if query_level != "resource" and query_level != "dataset":
        return make_response(jsonify([]))
    if query_level == "resource":
        resources = model.get_resources(request.json['ids'])
        return jsonify(resources)
    else:
        dataset = model.get_dataset(request.json['ids'])
        return jsonify(dataset)


# @app.route('/api/v1/ogdse/search/<query_level>')
# def get_resources_or_dataset_ids_(query_level):
#     interval_start = request.args.get("interval_start")
#     interval_end = request.args.get("interval_end")
#     gid_place = request.args.get('gid_place')
#     name_place = request.args.get('name_place')
#
#     if query_level != "resource" and query_level != "dataset":
#         return make_response(jsonify([]))
#     if gid_place or name_place:
#         if name_place and not gid_place:
#             places = spatial.find_places(name_place)
#             if len(places) > 1:
#                 return make_response(jsonify(places, 300))
#             elif len(places) == 1:
#                 resources_or_dataset_spatial_query = spatial.get_resources(places[0]['gid']) \
#                     if query_level == 'resource' else spatial.get_dataset(places[0]['gid'])
#             else:
#                 return make_response(jsonify([], 200))
#         else:
#             resources_or_dataset_spatial_query = spatial.get_resources(gid_place)\
#                 if query_level == 'resource' else spatial.get_dataset(gid_place)
#         if len(resources_or_dataset_spatial_query) == 0:
#             return make_response(jsonify([], 200))
#
#     if interval_start and interval_end:
#         interval_start = datetime.strptime(interval_start, "%d/%m/%Y").date()
#         interval_end = datetime.strptime(interval_end, "%d/%m/%Y").date()
#         resources_or_dataset_temporal_query = temporal.get_resource(interval_start, interval_end)\
#             if query_level == 'resource' else temporal.get_dataset(interval_start, interval_end)
#         if len(resources_or_dataset_temporal_query) == 0:
#             return make_response(jsonify([]))
#     results_queries = [res for res in [resources_or_dataset_temporal_query, resources_or_dataset_spatial_query]
#                        if len(res) != 0]
#
#     results_queries.sort(key=lambda elem: len(elem))
#
#     resources_or_dataset_dict = {}
#
#     for result in results_queries[1:]:
#         for resource_or_dataset_id in results_queries[0]:
#             gt_dict_freq = result.get(resource_or_dataset_id)
#             if gt_dict_freq:
#                 resources_or_dataset_dict[resource_or_dataset_id] = \
#                     (gt_dict_freq + results_queries[0][resource_or_dataset_id])/2
#         results_queries[0] = resources_or_dataset_dict
#     response = [{"id": key, "freq": results_queries[0][key]} for key in results_queries[0].keys()]
#     response.sort(key=lambda elem: elem["freq"], reverse=True)
#     response = make_response(jsonify(response, 200))
#     return response
