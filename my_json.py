import json
def obj_to_ba(input):
    return json.dumps(input).encode('utf-8')
def ba_to_obj(ba):
    return json.loads(ba.decode('utf-8'))

