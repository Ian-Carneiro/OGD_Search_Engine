from flask import Flask
from flask import jsonify, request, make_response
from datetime import datetime
from multiprocessing import Process, Manager
from app import temporal
from app import spatial
from app import thematic
from app import model
from time import time

app = Flask(__name__)
app.config['DEBUG'] = True


@app.route('/api/v1/ogdse/search/<query_level>')
def get_resources_or_dataset_ids_(query_level):
    interval_start = request.args.get("interval_start")
    interval_end = request.args.get("interval_end")
    gid_place = request.args.get('gid_place')
    name_place = request.args.get('name_place')
    topic = request.args.get('topic')

    if query_level not in ['resource', 'dataset'] or (not interval_start or not interval_end) and not gid_place and \
            not name_place and not topic:
        return make_response(jsonify({'msgErro': 'Não é possível realizar a consulta sem os parâmetros certos.'}, 400))
    # ----------------------------------------------------------------------------------------------------------------#
    if name_place and not gid_place:
        places = spatial.find_places(name_place)
        if len(places) > 1:
            return make_response(jsonify(places), 300)
        elif len(places) == 0:
            return make_response(jsonify(places), 200)
        else:
            gid_place = places[0]['gid']
    # ----------------------------------------------------------------------------------------------------------------#
    if interval_start and interval_end:
        try:
            interval_start = datetime.strptime(interval_start, "%Y-%m-%d").date()
            interval_end = datetime.strptime(interval_end, "%Y-%m-%d").date()
        except ValueError:
            return make_response(jsonify({'msgErro': 'Formato da(s) data(s) inválidos.'}), 400)

        if interval_start > interval_end:
            return make_response(jsonify({'msgErro': 'Data inicial maior que data final.'}), 400)
    # ----------------------------------------------------------------------------------------------------------------#
    with Manager() as manager:
        processes = []
        if gid_place:
            spatial_query_return = manager.dict()
            processes.append(Process(target=spatial_query, args=[gid_place, query_level, spatial_query_return]))

        if interval_start and interval_end:
            temporal_query_return = manager.dict()
            processes.append(Process(target=temporal_query,
                                     args=[interval_start, interval_end, query_level, temporal_query_return]))

        if topic:
            thematic_query_return = manager.dict()
            processes.append(Process(target=thematic_query, args=[topic, query_level, thematic_query_return]))

        for p in processes:
            p.start()
        for p in processes:
            p.join()
    # ----------------------------------------------------------------------------------------------------------------#
        results_queries = []
        if 'spatial_query_return' in locals():
            results_queries.append(spatial_query_return)
        if 'temporal_query_return' in locals():
            results_queries.append(temporal_query_return)
        if 'thematic_query_return' in locals():
            results_queries.append(thematic_query_return)
        results_queries.sort(key=lambda elem: len(elem))
    # ----------------------------------------------------------------------------------------------------------------#
        resources_or_dataset_dict = {}
        lt_dict_freq = results_queries[0]
        for result in results_queries[1:]:
            for resource_or_dataset_id in lt_dict_freq:
                gt_dict_freq = result.get(resource_or_dataset_id)
                if gt_dict_freq:
                    resources_or_dataset_dict[resource_or_dataset_id] = \
                        (gt_dict_freq + lt_dict_freq[resource_or_dataset_id]) / 2
            # print(lt_dict_freq)
            lt_dict_freq = resources_or_dataset_dict
            resources_or_dataset_dict = {}
    # ----------------------------------------------------------------------------------------------------------------#
        response = [{"id": key, "freq": lt_dict_freq[key]} for key in lt_dict_freq.keys()]
        response.sort(key=lambda elem: elem["freq"], reverse=True)
        response = make_response(jsonify(response), 200)
        return response


def spatial_query(gid_place, query_level, spatial_query_return):
    time_i = time()
    spatial_query_return.update(spatial.get_resources(gid_place)
                                if query_level == 'resource' else spatial.get_dataset(gid_place))
    print('tempo de execução(spatial_query):', time()-time_i)


def temporal_query(interval_start, interval_end, query_level, temporal_query_return):
    time_i = time()
    temporal_query_return.update(temporal.get_resource(interval_start, interval_end)
                                 if query_level == 'resource' else temporal.get_dataset(interval_start, interval_end))
    print('tempo de execução(temporal_query):', time() - time_i)


def thematic_query(topic, query_level, thematic_query_return):
    time_i = time()
    thematic_query_return.update(thematic.get_resource(topic)
                                 if query_level == 'resource' else thematic.get_dataset(topic))
    print('tempo de execução(thematic_query):', time() - time_i)


@app.route('/api/v1/ogdse/search/<query_level>', methods=['POST'])
def get_resources_or_dataset(query_level):
    if query_level != "resource" and query_level != "dataset":
        return make_response(jsonify([], 400))
    if query_level == "resource":
        resources = model.get_resources(request.json['ids'])
        return jsonify(resources)
    else:
        dataset = model.get_dataset(request.json['ids'])
        return jsonify(dataset)
